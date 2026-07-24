# ============================================================================
# TP3 - Sistemas Distribuidos
# CLUSTER SYNC - No de sincronizacao (5 nos), HERDADO DO TP2 (Alternativa 3)
#
# O protocolo de exclusao mutua e EXATAMENTE o do TP2:
#   - Todo pedido vira um ACQUIRE publicado no topico R_topic (fanout).
#   - O broker garante a MESMA ORDEM FIFO de entrega para todos os nos.
#   - Cada no mantem sua fila local PRIVADA F; quando o ACQUIRE do topo
#     e o seu, o no entra na secao critica; ao sair publica RELEASE.
#
# O QUE MUDOU NO TP3: dentro da secao critica o no NAO da mais um sleep
# simulando o acesso a R. Agora ele ACESSA DE VERDADE o CLUSTER STORE:
#   - Escolhe um elemento ALEATORIO do Cluster Store a cada entrada na
#     secao critica (requisito da proposta + pontos extras);
#   - Envia a escrita via HTTP puro (sem middleware) com TIMEOUT;
#   - Se o no do Store nao responde (falha por queda/omissao), RETENTA
#     em outro elemento aleatorio - e a deduplicacao por req_id no Store
#     garante que a escrita nunca e aplicada duas vezes.
# ============================================================================

import json
import os
import random
import sys
import threading
import time

import pika
import requests
from colorama import Fore, init

init(autoreset=True)

# ----------------------------------------------------------------------------
# Configuracao (argv mantem compatibilidade com o TP2; env e usado nos
# containers do docker compose / Kubernetes)
# ----------------------------------------------------------------------------

def _derive_id_from_hostname():
    """No k8s (StatefulSet) o hostname e 'sync-0'... => ID = ordinal + 1."""
    import re, socket
    m = re.match(r'.*-(\d+)$', socket.gethostname().split('.')[0])
    return int(m.group(1)) + 1 if m else random.randint(1000, 9999)

NODE_ID = (sys.argv[1] if len(sys.argv) > 1 else
           os.environ.get('NODE_ID') or str(_derive_id_from_hostname()))
RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'admin123')

# Elementos do Cluster Store (a posicao na lista define o ID: pos 0 => ID 1)
STORE_NODES = os.environ.get('STORE_NODES', 'localhost:6001,localhost:6002,localhost:6003')
STORES = []   # [(id, "http://host:porta")]
for idx, hostport in enumerate([h.strip() for h in STORE_NODES.split(',') if h.strip()]):
    STORES.append((idx + 1, f'http://{hostport}'))

WRITE_TIMEOUT = float(os.environ.get('WRITE_TIMEOUT', 2.5))  # timeout por tentativa
MAX_ATTEMPTS = int(os.environ.get('MAX_ATTEMPTS', 8))        # tentativas de escrita
RETRY_DELAY = float(os.environ.get('RETRY_DELAY', 0.4))      # espera entre tentativas
DEAD_TTL = float(os.environ.get('DEAD_TTL', 5.0))            # qto tempo evitar um Store que falhou

# Stores que ESTE sync descobriu estarem mortos/omissos (id -> ate quando evitar).
# Evita re-sortear um no caido a cada tentativa (nada de bolinha indo pro no morto).
_dead_stores = {}

# AUTO_START=1 (padrao nos containers): o no fica pronto sozinho apos alguns
# segundos, sem esperar o START_SIMULATION do run_simulation.py do TP2.
AUTO_START = os.environ.get('AUTO_START', '1') == '1'

F = []              # Fila local do protocolo (PRIVADA ao no) - herdada do TP2
pending_rpc = {}    # req_id -> props do RPC (para responder ao cliente depois)
cluster_ready = False

def log(msg, color=Fore.WHITE):
    print(color + f"[Sync {NODE_ID}] {msg}", flush=True)

# ----------------------------------------------------------------------------
# Conexao com o broker (herdado do TP2, com retry para ambientes docker/k8s
# onde o RabbitMQ pode demorar a subir)
# ----------------------------------------------------------------------------

def conectar():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials,
                                       heartbeat=120, blocked_connection_timeout=60)
    while True:
        try:
            return pika.BlockingConnection(params)
        except Exception as e:
            log(f"RabbitMQ indisponivel ({e}). Tentando de novo em 2s...", Fore.YELLOW)
            time.sleep(2)

def setup_rabbit(channel):
    # A proposta do TP2 exige um topico denominado R_topic
    channel.exchange_declare(exchange='R_topic', exchange_type='fanout')
    # Exchange de telemetria para o Dashboard Visual
    channel.exchange_declare(exchange='dashboard_topic', exchange_type='fanout')
    # Fila exclusiva para escutar os eventos de sincronizacao
    result = channel.queue_declare(queue='', exclusive=True)
    sync_queue = result.method.queue
    channel.queue_bind(exchange='R_topic', queue=sync_queue)
    # Fila para receber requisicoes dos clientes (RPC)
    rpc_queue_name = f'rpc_queue_{NODE_ID}'
    channel.queue_declare(queue=rpc_queue_name)
    # Limpa pedidos antigos que sobraram na fila (ex.: de antes de um reset).
    # Sem isso, apos "rollout restart" o sync reprocessa o backlog e aparecem
    # bolinhas "do nada" depois de um tempo.
    try:
        channel.queue_purge(rpc_queue_name)
    except Exception:
        pass
    return sync_queue, rpc_queue_name

def dash(channel, payload):
    """Publica telemetria para o dashboard (best-effort)."""
    try:
        channel.basic_publish(exchange='dashboard_topic', routing_key='',
                              body=json.dumps(payload))
    except Exception:
        pass

# ----------------------------------------------------------------------------
# NOVO DO TP3: acesso REAL ao recurso R no Cluster Store
# ----------------------------------------------------------------------------

def access_store(channel, pedido_cliente):
    """Executa a escrita no Cluster Store dentro da secao critica.

    - Sorteia um elemento do Store a cada tentativa (pode variar de elemento
      a cada entrada na secao critica, como diz a proposta).
    - Timeout + retentativa cobrem os casos de falha 2.1/2.2/2.3: se o no
      contatado caiu (ou o primario caiu no meio da escrita), tentamos em
      outro no; o req_id garante idempotencia no Store.
    """
    resource = pedido_cliente.get('resource', 'R1')
    payload = {
        'req_id': pedido_cliente['req_id'],
        'resource': resource,
        'amount': 1,
        'client_id': pedido_cliente.get('client_id'),
        'sync_id': NODE_ID,
        'client_ts': pedido_cliente.get('timestamp'),
    }

    for attempt in range(1, MAX_ATTEMPTS + 1):
        # Sorteia entre os Stores que NAO estao marcados como mortos. Assim a
        # escrita nao vai (nem anima bolinha) pra um no que ja sabemos estar
        # caido. Se todos estiverem marcados, tenta qualquer um (o TTL expira).
        now = time.time()
        vivos = [(sid, url) for sid, url in STORES
                 if _dead_stores.get(sid, 0) < now]
        store_id, store_url = random.choice(vivos or STORES)  # ESCOLHA ALEATORIA
        dash(channel, {"type": "STORE_WRITE", "node_id": NODE_ID,
                       "store_id": store_id, "resource": resource,
                       "attempt": attempt})
        log(f"Secao critica: escrevendo {resource} no Store {store_id} "
            f"(tentativa {attempt}/{MAX_ATTEMPTS})...", Fore.CYAN)
        try:
            r = requests.post(f'{store_url}/write', json=payload,
                              timeout=WRITE_TIMEOUT)
            body = r.json()
            if r.status_code == 200 and body.get('status') == 'COMMITTED':
                _dead_stores.pop(store_id, None)  # respondeu: esta vivo
                via = body.get('routed_via')
                rota = f"entrou pelo {store_id}" + \
                       (f", repassada ao primario {body.get('primary_id')}"
                        if via else f" (era o primario)")
                log(f"COMMIT no Store: {resource}={body.get('value')} "
                    f"seq={body.get('seq')} [{rota}] ACKs={body.get('replicas_ack')}",
                    Fore.GREEN)
                dash(channel, {"type": "STORE_WRITE_OK", "node_id": NODE_ID,
                               "store_id": store_id,
                               "primary_id": body.get('primary_id'),
                               "resource": resource, "seq": body.get('seq'),
                               "value": body.get('value'),
                               "acks": body.get('replicas_ack', []),
                               "attempts": attempt,
                               "dedup": body.get('dedup', False)})
                return body
            # 409 IN_FLIGHT / 503 eleicao em curso / SYNCING etc: retenta
            # (nao marca como morto - o no esta vivo, so ocupado)
            log(f"Store {store_id} nao commitou ({r.status_code} "
                f"{body.get('error', '')}). Retentando...", Fore.YELLOW)
        except Exception as e:
            # Timeout/conexao recusada => no morto ou em omissao: marca pra
            # evitar nas proximas tentativas (nao insiste no no caido).
            _dead_stores[store_id] = time.time() + DEAD_TTL
            log(f"FALHA no Store {store_id} (timeout/queda): {type(e).__name__}. "
                f"Marcando como morto e indo pra outro no...", Fore.RED)
        dash(channel, {"type": "STORE_WRITE_FAIL", "node_id": NODE_ID,
                       "store_id": store_id, "attempt": attempt,
                       "resource": resource})
        time.sleep(RETRY_DELAY)

    log("ERRO: Cluster Store inteiro indisponivel. Escrita nao realizada.", Fore.RED)
    return None

# ----------------------------------------------------------------------------
# Protocolo de exclusao mutua (Alternativa 3) - herdado do TP2
# ----------------------------------------------------------------------------

def process_F(channel, connection):
    global F, cluster_ready
    if not cluster_ready or not F:
        return

    top = F[0]

    # O protocolo diz: avalia a fila F. Se o ACQUIRE do topo e meu, posso entrar.
    if top['node_id'] == NODE_ID and top['req_id'] in pending_rpc:
        pedido = top.get('pedido_cliente', {})
        client_id = pedido.get('client_id', 'Desconhecido')
        resource = pedido.get('resource', 'R1')
        log(f"Entrando na Secao Critica (Cliente {client_id} -> {resource})", Fore.GREEN)
        dash(channel, {"type": "NODE_ENTER", "node_id": NODE_ID,
                       "client_id": client_id, "resource": resource})

        # ===== TP3: acesso REAL ao recurso replicado (era um sleep no TP2) =====
        resultado = access_store(channel, pedido)
        # =======================================================================

        log("Saindo da Secao Critica.", Fore.RED)
        dash(channel, {"type": "NODE_EXIT", "node_id": NODE_ID,
                       "client_id": client_id, "resource": resource,
                       "ok": resultado is not None})

        props = pending_rpc.pop(top['req_id'])

        # Envia o RELEASE para o broker (todos os nos removem o pedido de F)
        msg_release = {'action': 'RELEASE', 'node_id': NODE_ID,
                       'pedido_cliente': pedido}
        channel.basic_publish(exchange='R_topic', routing_key='',
                              body=json.dumps(msg_release))

        # Responde ao cliente (RPC) com o resultado REAL da escrita
        if props.reply_to:
            if resultado:
                resposta = {'status': 'COMMITTED',
                            'resource': resource,
                            'value': resultado.get('value'),
                            'seq': resultado.get('seq'),
                            'primary_id': resultado.get('primary_id'),
                            'replicas_ack': resultado.get('replicas_ack', [])}
            else:
                resposta = {'status': 'FAILED', 'resource': resource,
                            'motivo': 'Cluster Store indisponivel'}
            channel.basic_publish(
                exchange='', routing_key=props.reply_to,
                properties=pika.BasicProperties(correlation_id=props.correlation_id),
                body=json.dumps(resposta))

def rpc_consumer_thread():
    """Thread dedicada para ler os pedidos dos clientes IMEDIATAMENTE,
    mesmo enquanto a thread principal esta ocupada na secao critica."""
    while True:
        try:
            conn = conectar()
            ch = conn.channel()
            rpc_queue_name = f'rpc_queue_{NODE_ID}'
            ch.queue_declare(queue=rpc_queue_name)

            def on_rpc(ch, method, props, body):
                pedido_cliente = json.loads(body)
                req_id = pedido_cliente.get('req_id')
                client_id = pedido_cliente.get('client_id')
                resource = pedido_cliente.get('resource', 'R1')
                log(f"Pedido do Cliente {client_id} ({resource}, req {str(req_id)[:8]}). "
                    f"Publicando ACQUIRE...", Fore.CYAN)
                pending_rpc[req_id] = props
                msg_acquire = {'action': 'ACQUIRE', 'node_id': NODE_ID,
                               'pedido_cliente': pedido_cliente, 'req_id': req_id}
                ch.basic_publish(exchange='R_topic', routing_key='',
                                 body=json.dumps(msg_acquire))
                ch.basic_ack(delivery_tag=method.delivery_tag)

            ch.basic_consume(queue=rpc_queue_name, on_message_callback=on_rpc)
            log(f"Thread RPC pronta (fila {rpc_queue_name}).", Fore.YELLOW)
            ch.start_consuming()
        except Exception as e:
            log(f"Thread RPC caiu ({e}). Reconectando em 2s...", Fore.RED)
            time.sleep(2)

def on_sync_message(ch, method, props, body):
    """Recebe ACQUIRE/RELEASE do topico R_topic (mesma logica do TP2)."""
    global F, cluster_ready
    msg = json.loads(body)

    if msg['action'] == 'ACQUIRE':
        # Ordena a fila F pelo timestamp do pedido original do cliente
        F.append(msg)
        F.sort(key=lambda x: x.get('pedido_cliente', {}).get('timestamp', 0))
    elif msg['action'] == 'RELEASE':
        req_id_to_remove = msg.get('pedido_cliente', {}).get('req_id')
        F = [req for req in F if req['req_id'] != req_id_to_remove]
    elif msg['action'] == 'START_SIMULATION':
        cluster_ready = True

    ch.basic_ack(delivery_tag=method.delivery_tag)

    if cluster_ready:
        process_F(ch, ch.connection)

def _auto_start_timer(connection, channel):
    """Com AUTO_START=1 o cluster fica pronto sozinho (modo containers)."""
    global cluster_ready
    time.sleep(4.0)
    if not cluster_ready:
        cluster_ready = True
        log("Cluster pronto (AUTO_START).", Fore.YELLOW)
        # Processa qualquer pedido que ja esteja na fila F (thread-safe no pika)
        try:
            connection.add_callback_threadsafe(
                lambda: process_F(channel, connection))
        except Exception:
            pass

def main():
    log(f"Iniciando No do Cluster Sync... (broker={RABBITMQ_HOST}, "
        f"stores={[s[1] for s in STORES]})", Fore.YELLOW)
    connection = conectar()
    channel = connection.channel()
    sync_queue, rpc_queue_name = setup_rabbit(channel)

    # 1. Escuta os eventos do cluster (ACQUIRE/RELEASE)
    channel.basic_consume(queue=sync_queue, on_message_callback=on_sync_message)

    # 2. Aguarda formacao do cluster
    log("Aguardando formacao do cluster (2s)...", Fore.YELLOW)
    time.sleep(2.0)

    # 3. Thread que consome os pedidos dos clientes em paralelo
    threading.Thread(target=rpc_consumer_thread, daemon=True).start()

    # 4. Auto-start (nos containers nao existe o run_simulation.py do TP2)
    if AUTO_START:
        threading.Thread(target=_auto_start_timer, args=(connection, channel),
                         daemon=True).start()

    log(f"No operante. Clientes na fila {rpc_queue_name}; sincronizacao na {sync_queue}",
        Fore.YELLOW)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()

if __name__ == "__main__":
    main()
