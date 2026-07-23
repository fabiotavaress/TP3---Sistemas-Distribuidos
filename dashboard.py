# ============================================================================
# TP3 - Sistemas Distribuidos
# DASHBOARD - Painel visual em tempo real (herdado do TP2 e ampliado)
#
# Fontes de dados:
#   1. RabbitMQ 'dashboard_topic'  -> eventos dos CLIENTES e do CLUSTER SYNC
#      (CLIENT_REQ, NODE_ENTER, STORE_WRITE, STORE_WRITE_OK/FAIL, NODE_EXIT)
#   2. HTTP POST /event            -> eventos do CLUSTER STORE
#      (ELECTION, REPLICATE, REPLICATE_ACK, FORWARD, PEER_DEAD, RESYNC, ...)
#   3. Poller HTTP GET /status     -> estado consolidado das 3 replicas
#      (papel, epoca, seq, versao de cada recurso => grade de consistencia)
#
# Tudo e retransmitido aos navegadores via SSE (Server-Sent Events).
# O dashboard tambem e o "painel de controle" das falhas: os botoes do
# front chamam /store/<id>/fail|crash|recover, que sao repassados ao no.
# ============================================================================

import json
import os
import queue
import threading
import time
import uuid

import pika
import requests
from flask import Flask, Response, jsonify, render_template, request

app = Flask(__name__)

RABBITMQ_HOST = os.environ.get('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.environ.get('RABBITMQ_USER', 'admin')
RABBITMQ_PASS = os.environ.get('RABBITMQ_PASS', 'admin123')

STORE_NODES = os.environ.get('STORE_NODES', 'localhost:6001,localhost:6002,localhost:6003')
STORES = {}
for idx, hostport in enumerate([h.strip() for h in STORE_NODES.split(',') if h.strip()]):
    STORES[idx + 1] = f'http://{hostport}'

RESOURCES = [r.strip() for r in os.environ.get('RESOURCES', 'R1,R2,R3,R4,R5').split(',')]
POLL_INTERVAL = float(os.environ.get('POLL_INTERVAL', 0.7))

# Navegadores conectados via SSE
listeners = []
listeners_lock = threading.Lock()

# Fila thread-safe para publicar no RabbitMQ (cliques do publico)
publish_queue = queue.Queue()
total_clicks = 0

def broadcast(event):
    """Envia um evento SSE para todos os navegadores conectados."""
    msg = json.dumps(event)
    with listeners_lock:
        for l in listeners:
            l.append(msg)

# ----------------------------------------------------------------------------
# 1) Consumidor RabbitMQ: telemetria dos clientes e do Cluster Sync
# ----------------------------------------------------------------------------

def pika_consumer():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            channel.exchange_declare(exchange='dashboard_topic', exchange_type='fanout')
            result = channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange='dashboard_topic', queue=queue_name)

            def callback(ch, method, properties, body):
                with listeners_lock:
                    for l in listeners:
                        l.append(body.decode('utf-8'))

            print("[Dashboard] Conectado ao RabbitMQ. Escutando telemetria...", flush=True)
            channel.basic_consume(queue=queue_name, on_message_callback=callback,
                                  auto_ack=True)
            channel.start_consuming()
        except Exception as e:
            print(f"[Dashboard] Reconectando consumidor RabbitMQ... ({e})", flush=True)
            time.sleep(2)

def pika_publisher():
    """Thread dedicada para os pedidos vindos de cliques no navegador."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    while True:
        try:
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            while True:
                task = publish_queue.get()
                if task['type'] == 'FAKE_TELEMETRY':
                    channel.exchange_declare(exchange='dashboard_topic',
                                             exchange_type='fanout')
                    channel.basic_publish(exchange='dashboard_topic', routing_key='',
                                          body=json.dumps(task['payload']))
                elif task['type'] == 'REAL_REQ':
                    channel.queue_declare(queue=task['queue'])
                    channel.basic_publish(exchange='', routing_key=task['queue'],
                                          body=json.dumps(task['payload']))
        except Exception as e:
            print(f"[Dashboard] Reconectando publicador RabbitMQ... ({e})", flush=True)
            time.sleep(2)

# ----------------------------------------------------------------------------
# 2) Poller do Cluster Store: consolida o estado das 3 replicas
# ----------------------------------------------------------------------------

def store_poller():
    while True:
        snapshot = {'type': 'STORE_STATE', 'ts': time.time(), 'stores': {},
                    'resources': RESOURCES}
        for sid, url in STORES.items():
            try:
                r = requests.get(f'{url}/status', timeout=0.6)
                info = r.json()
                info['reachable'] = True
                snapshot['stores'][str(sid)] = info
            except Exception:
                # Processo realmente morto (crash/pod deletado): nem o canal
                # administrativo responde.
                snapshot['stores'][str(sid)] = {'id': sid, 'reachable': False,
                                                'role': 'MORTO', 'failed': True}
        # Verificacao de consistencia entre as replicas vivas
        versions = {}
        for res in RESOURCES:
            vs = set()
            for sid, info in snapshot['stores'].items():
                if info.get('reachable') and not info.get('failed') \
                        and not info.get('syncing'):
                    rec = (info.get('resources') or {}).get(res)
                    if rec:
                        vs.add((rec.get('version'), rec.get('value')))
            versions[res] = (len(vs) <= 1)
        snapshot['consistent'] = all(versions.values())
        snapshot['consistent_by_resource'] = versions
        broadcast(snapshot)
        time.sleep(POLL_INTERVAL)

# ----------------------------------------------------------------------------
# Rotas HTTP
# ----------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/stream")
def stream():
    """SSE consumido pelo navegador."""
    def event_stream():
        q = []
        with listeners_lock:
            listeners.append(q)
        try:
            while True:
                if q:
                    msg = q.pop(0)
                    yield f"data: {msg}\n\n"
                else:
                    time.sleep(0.05)
        except GeneratorExit:
            with listeners_lock:
                if q in listeners:
                    listeners.remove(q)
    return Response(event_stream(), content_type="text/event-stream")

@app.route("/event", methods=["POST"])
def store_event():
    """Eventos instantaneos vindos dos nos do Cluster Store (HTTP puro)."""
    ev = request.get_json(silent=True) or {}
    broadcast(ev)
    return jsonify({"status": "ok"})

@app.route("/force_request/<client_id>", methods=["POST"])
def force_request(client_id):
    """Clique em um cliente no front: injeta um pedido REAL no sistema.
    O recurso tambem e sorteado aqui (mesma regra dos clientes reais)."""
    global total_clicks
    total_clicks += 1
    try:
        import random
        node_num = client_id.replace("C", "")
        resource = random.choice(RESOURCES)
        dash_event = {"type": "CLIENT_REQ", "client_id": client_id,
                      "node_id": node_num, "resource": resource, "click": True}
        publish_queue.put({"type": "FAKE_TELEMETRY", "payload": dash_event})
        msg = {'client_id': client_id, 'timestamp': time.time(),
               'req_id': str(uuid.uuid4()), 'resource': resource}
        publish_queue.put({"type": "REAL_REQ", "queue": f'rpc_queue_{node_num}',
                           "payload": msg})
        return jsonify({"status": "ok", "resource": resource})
    except Exception as e:
        print("Erro ao forcar request:", e, flush=True)
        return jsonify({"status": "error"})

@app.route("/get_total_clicks", methods=["GET"])
def get_total_clicks():
    return jsonify({"count": total_clicks})

# ----------------------------------------------------------------------------
# Painel de controle de falhas (proxy para os nos do Store)
# ----------------------------------------------------------------------------

@app.route("/store/<int:sid>/fail", methods=["POST"])
def store_fail(sid):
    """Injeta falha por OMISSAO no no <sid> (opcional: ?duration=20)."""
    if sid not in STORES:
        return jsonify({"error": "no inexistente"}), 404
    body = request.get_json(silent=True) or {}
    try:
        r = requests.post(f'{STORES[sid]}/admin/fail', json=body, timeout=2)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

@app.route("/store/<int:sid>/crash", methods=["POST"])
def store_crash(sid):
    """Mata o processo do no <sid> (falha por QUEDA real)."""
    if sid not in STORES:
        return jsonify({"error": "no inexistente"}), 404
    try:
        r = requests.post(f'{STORES[sid]}/admin/crash', timeout=2)
        return jsonify(r.json())
    except Exception as e:
        # O processo pode morrer antes de responder - isso e esperado
        return jsonify({"status": "crashing", "note": str(e)})

@app.route("/store/<int:sid>/recover", methods=["POST"])
def store_recover(sid):
    if sid not in STORES:
        return jsonify({"error": "no inexistente"}), 404
    try:
        r = requests.post(f'{STORES[sid]}/admin/recover', timeout=2)
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

if __name__ == "__main__":
    threading.Thread(target=pika_consumer, daemon=True).start()
    threading.Thread(target=pika_publisher, daemon=True).start()
    threading.Thread(target=store_poller, daemon=True).start()
    print("[Dashboard] Servidor web na porta 5000...", flush=True)
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
