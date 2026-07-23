# ============================================================================
# TP3 - Sistemas Distribuidos
# CLIENTE - herdado do TP2, com a novidade pedida pelo professor:
#
#   >>> A CADA REQUISICAO O CLIENTE ESCOLHE UM RECURSO ALEATORIO <<<
#   >>> (R1..R5) PARA ESCREVER NO CLUSTER STORE                  <<<
#
# O restante segue o TP2: cada cliente conhece APENAS UM elemento do
# Cluster Sync (RPC via fila rpc_queue_<no>), envia client_id + timestamp
# + req_id unico, faz de 10 a 50 acessos e dorme de 1 a 5s apos cada
# COMMITTED recebido.
# ============================================================================

import json
import os
import random
import sys
import time
import uuid

import pika
from colorama import Fore, init

init(autoreset=True)

def _derive_id_from_hostname():
    """No k8s (StatefulSet) o hostname e 'client-0'... => C1, C2, ..."""
    import re, socket
    m = re.match(r'.*-(\d+)$', socket.gethostname().split('.')[0])
    return f"C{int(m.group(1)) + 1}" if m else f"C{str(uuid.uuid4())[:4]}"

CLIENT_ID = (sys.argv[1] if len(sys.argv) > 1 else
             os.environ.get('CLIENT_ID') or _derive_id_from_hostname())
# Cada cliente conhece APENAS UM elemento do Cluster Sync (requisito TP2)
NODE_ID = (sys.argv[2] if len(sys.argv) > 2 else
           os.environ.get('NODE_ID') or CLIENT_ID.replace('C', '') or '1')

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'admin123')

# Conjunto de recursos replicados no Cluster Store
RESOURCES = [r.strip() for r in os.environ.get('RESOURCES', 'R1,R2,R3,R4,R5').split(',')]

# CLIENT_LOOP=1 (padrao nos containers): ao terminar um lote de 10-50
# requisicoes, descansa e comeca outro - mantem a demonstracao viva.
CLIENT_LOOP = os.environ.get('CLIENT_LOOP', '1') == '1'
RESPONSE_TIMEOUT = float(os.environ.get('RESPONSE_TIMEOUT', 120.0))

def log(msg, color=Fore.WHITE):
    print(color + f"[Client {CLIENT_ID}] {msg}", flush=True)

class SyncClient:
    def __init__(self):
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        params = pika.ConnectionParameters(host=RABBITMQ_HOST,
                                           credentials=credentials,
                                           heartbeat=120)
        while True:
            try:
                self.connection = pika.BlockingConnection(params)
                break
            except Exception as e:
                log(f"RabbitMQ indisponivel ({e}). Retentando em 2s...", Fore.YELLOW)
                time.sleep(2)
        self.channel = self.connection.channel()

        # Fila exclusiva para receber a resposta (COMMITTED) do Cluster Sync
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.reply_queue = result.method.queue
        self.channel.exchange_declare(exchange='dashboard_topic',
                                      exchange_type='fanout')
        self.channel.basic_consume(queue=self.reply_queue,
                                   on_message_callback=self.on_response,
                                   auto_ack=True)
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = json.loads(body)

    def acquire_resource(self):
        """Envia UMA requisicao de escrita. AQUI acontece a escolha
        aleatoria do recurso (ponto extra do professor)."""
        self.response = None
        self.corr_id = str(uuid.uuid4())
        req_id = str(uuid.uuid4())
        resource = random.choice(RESOURCES)   # <== RECURSO ALEATORIO!

        msg = {
            'client_id': CLIENT_ID,
            'timestamp': time.time(),   # garante ordenacao e pedidos unicos
            'req_id': req_id,
            'resource': resource,
        }

        # Telemetria para o dashboard animar a topologia
        dash_event = {"type": "CLIENT_REQ", "client_id": CLIENT_ID,
                      "node_id": NODE_ID, "resource": resource}
        self.channel.basic_publish(exchange='dashboard_topic', routing_key='',
                                   body=json.dumps(dash_event))

        self.channel.basic_publish(
            exchange='',
            routing_key=f'rpc_queue_{NODE_ID}',
            properties=pika.BasicProperties(reply_to=self.reply_queue,
                                            correlation_id=self.corr_id),
            body=json.dumps(msg))

        # Espera pela resposta (com timeout de seguranca)
        deadline = time.time() + RESPONSE_TIMEOUT
        while self.response is None and time.time() < deadline:
            self.connection.process_data_events(time_limit=1.0)
        return self.response, resource

def run_batch(client):
    """Um lote de 10 a 50 acessos, como manda a proposta do TP2."""
    num_requests = random.randint(10, 50)
    log(f"Novo lote: {num_requests} requisicoes de escrita "
        f"(recursos possiveis: {RESOURCES}).", Fore.CYAN)
    ok = 0
    for i in range(num_requests):
        log(f"Pedido {i+1}/{num_requests}: sorteando recurso...", Fore.YELLOW)
        res, resource = client.acquire_resource()

        if res is None:
            log(f"SEM RESPOSTA para {resource} (timeout). Seguindo em frente.",
                Fore.RED)
        elif res.get('status') == 'COMMITTED':
            ok += 1
            log(f"COMMITTED: {resource}={res.get('value')} "
                f"(seq global {res.get('seq')}, primario Store {res.get('primary_id')}, "
                f"ACKs {res.get('replicas_ack')})", Fore.GREEN)
        else:
            log(f"FALHOU: {res.get('motivo', res)}", Fore.RED)

        wait_time = random.uniform(1, 5)   # espera de 1 a 5s (requisito)
        log(f"Dormindo por {wait_time:.1f}s...\n")
        time.sleep(wait_time)
    log(f"Lote concluido: {ok}/{num_requests} escritas confirmadas.", Fore.MAGENTA)

def main():
    log(f"Iniciando. Conectado APENAS ao No {NODE_ID} do Cluster Sync.", Fore.MAGENTA)
    while True:
        try:
            client = SyncClient()
            run_batch(client)
            client.connection.close()
        except Exception as e:
            log(f"Conexao perdida ({type(e).__name__}: {e}). Reiniciando cliente...",
                Fore.RED)
            time.sleep(3)
            continue
        if not CLIENT_LOOP:
            break
        pausa = random.uniform(3, 8)
        log(f"Descansando {pausa:.0f}s antes do proximo lote...", Fore.MAGENTA)
        time.sleep(pausa)
    log("Cliente encerrado.", Fore.MAGENTA)

if __name__ == "__main__":
    main()
