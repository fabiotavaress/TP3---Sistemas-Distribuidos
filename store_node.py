# ============================================================================
# TP3 - Sistemas Distribuidos
# CLUSTER STORE - No de armazenamento replicado (3 replicas)
#
# Implementa o PROTOCOLO 1 da proposta (Primario-Backup, Figura 7.19):
#   W1. Requisicao de escrita chega em QUALQUER elemento do Store
#   W2. Se o elemento nao e o primario, REPASSA a requisicao ao primario
#   W3. O primario aplica localmente e DIZ AOS BACKUPS para atualizar
#   W4. Cada backup aplica e RECONHECE (ACK) a atualizacao
#   W5. O primario responde COMMITTED ao Cluster Sync
#
# Tolerancia a falhas (Opcao 2 da proposta - falha por queda/omissao):
#   - Mensagens de controle PING periodicas entre os 3 nos (heartbeat)
#   - Timeout de PING => no considerado MORTO
#   - Se o PRIMARIO morre => ELEICAO deterministica: menor ID vivo assume,
#     com numero de EPOCA crescente para resolver conflitos de visao
#   - No que volta a vida => re-sincroniza o estado via SNAPSHOT e vira backup
#
# IMPORTANTE: nenhum middleware e usado aqui. Toda a comunicacao do
# Cluster Store (Sync->Store e Store->Store) e feita com HTTP puro.
# ============================================================================

import json
import os
import re
import socket
import threading
import time
import queue
from collections import OrderedDict, deque

import requests
from flask import Flask, jsonify, request

# ----------------------------------------------------------------------------
# Configuracao (via variaveis de ambiente, com padroes para rodar local)
# ----------------------------------------------------------------------------

def _derive_id_from_hostname():
    """No Kubernetes (StatefulSet) o hostname e 'store-0', 'store-1', ...
    Derivamos o ID somando 1 ao ordinal (store-0 => ID 1)."""
    host = socket.gethostname().split('.')[0]
    m = re.match(r'.*-(\d+)$', host)
    if m:
        return int(m.group(1)) + 1
    return 1

MY_ID = int(os.environ.get('MY_ID', 0)) or _derive_id_from_hostname()
PORT = int(os.environ.get('PORT', 6000))

# Lista dos 3 nos do Cluster Store: "host1:porta,host2:porta,host3:porta"
# A posicao na lista define o ID (posicao 0 => ID 1)
STORE_NODES = os.environ.get('STORE_NODES', 'localhost:6001,localhost:6002,localhost:6003')
PEERS = {}   # {id: "http://host:porta"} de TODOS os nos (inclusive eu)
for idx, hostport in enumerate([h.strip() for h in STORE_NODES.split(',') if h.strip()]):
    PEERS[idx + 1] = f'http://{hostport}'

OTHERS = {nid: url for nid, url in PEERS.items() if nid != MY_ID}

# Recursos replicados. O cliente escolhe UM ALEATORIO a cada escrita (ponto extra)
RESOURCES = [r.strip() for r in os.environ.get('RESOURCES', 'R1,R2,R3,R4,R5').split(',')]

DASHBOARD_URL = os.environ.get('DASHBOARD_URL', '')  # telemetria (opcional)

# Temporizadores do protocolo (proposta: "Temporizadores nas comunicacoes
# podem ser necessarios. Ha a alternativa de envio periodico de PING")
HB_INTERVAL = float(os.environ.get('HB_INTERVAL', 0.8))      # intervalo entre PINGs
PEER_TIMEOUT = float(os.environ.get('PEER_TIMEOUT', 4.0))    # sem PING ha X s => morto
RECENT_WINDOW = float(os.environ.get('RECENT_WINDOW', 10.0)) # janela p/ replicacao
PING_TIMEOUT = float(os.environ.get('PING_TIMEOUT', 1.0))    # timeout da chamada PING
REPL_TIMEOUT = float(os.environ.get('REPL_TIMEOUT', 2.0))    # timeout do REPLICATE (W3/W4)
FORWARD_TIMEOUT = float(os.environ.get('FORWARD_TIMEOUT', 4.5))  # timeout do repasse (W2)
STARTUP_GRACE = float(os.environ.get('STARTUP_GRACE', 3.0))  # espera inicial p/ formar cluster
OMISSION_HANG = 12.0   # quanto tempo um no "em omissao" segura a resposta

# ----------------------------------------------------------------------------
# Estado do no
# ----------------------------------------------------------------------------

state_lock = threading.RLock()

resources = {r: {'value': 0, 'version': 0, 'last_writer': None,
                 'last_sync': None, 'last_ts': None} for r in RESOURCES}
seq = 0                    # numero de sequencia global do ultimo commit aplicado
epoch = 0                  # epoca da eleicao (cresce a cada novo primario)
primary_id = None          # quem eu ACHO que e o primario
applied_reqs = OrderedDict()   # req_id -> resultado (deduplicacao/idempotencia)
inflight_reqs = set()          # req_ids sendo processados AGORA (anti-corrida)
history = deque(maxlen=12)     # ultimas escritas (para o dashboard)

last_seen = {}             # peer_id -> timestamp do ultimo PING recebido/respondido
failed = False             # True = simulando FALHA POR OMISSAO (vivo, mas mudo)
syncing = True             # True = recuperando estado (ainda nao pode servir)
started_at = time.time()

counters = {
    'writes_committed': 0,   # escritas commitadas por MIM como primario
    'replicates_applied': 0, # replicacoes aplicadas por MIM como backup
    'forwards': 0,           # requisicoes repassadas ao primario (W2)
    'resyncs': 0,            # quantas vezes re-sincronizei via snapshot
    'elections': 0,          # eleicoes que EU venci
}

app = Flask(__name__)

def log(msg):
    print(f"[Store {MY_ID}] {msg}", flush=True)

# ----------------------------------------------------------------------------
# Telemetria para o Dashboard (fire-and-forget; nunca trava o protocolo)
# ----------------------------------------------------------------------------

_event_q = queue.Queue()

def emit(event_type, **kw):
    if not DASHBOARD_URL:
        return
    kw['type'] = event_type
    kw['store_id'] = MY_ID
    kw['ts'] = time.time()
    _event_q.put(kw)

def _event_sender():
    while True:
        ev = _event_q.get()
        try:
            requests.post(f'{DASHBOARD_URL}/event', json=ev, timeout=0.8)
        except Exception:
            pass  # dashboard fora do ar nao pode afetar o protocolo

# ----------------------------------------------------------------------------
# Simulacao de falha por OMISSAO: o processo esta vivo, mas para de responder
# aos endpoints do PROTOCOLO (write/replicate/ping/snapshot). Os endpoints
# administrativos (/admin, /status, /health) continuam vivos para o dashboard
# conseguir "reviver" o no e para o Kubernetes NAO reinicia-lo (queremos
# demonstrar a deteccao pela propria aplicacao, via timeout de PING).
# ----------------------------------------------------------------------------

def _omission_guard():
    """Se o no esta 'em omissao', segura a conexao ate estourar o timeout
    de quem chamou - exatamente o comportamento de uma falha por omissao."""
    if failed:
        time.sleep(OMISSION_HANG)
        return True
    return False

# ----------------------------------------------------------------------------
# Nucleo do estado replicado
# ----------------------------------------------------------------------------

def _apply_write(w_seq, req_id, res_name, record, result=None):
    """Aplica uma escrita no estado local (usado pelo primario e pelos backups)."""
    global seq
    with state_lock:
        resources[res_name] = record
        seq = w_seq
        if result is not None:
            applied_reqs[req_id] = result
        else:
            applied_reqs[req_id] = {'status': 'COMMITTED', 'seq': w_seq,
                                    'resource': res_name, 'value': record['value']}
        while len(applied_reqs) > 4000:   # limita memoria da deduplicacao
            applied_reqs.popitem(last=False)
        history.appendleft({'seq': w_seq, 'resource': res_name,
                            'value': record['value'],
                            'writer': record['last_writer'],
                            'sync': record['last_sync'], 'ts': record['last_ts']})

def _alive_ids():
    """IDs que EU considero vivos agora (eu + peers com PING recente)."""
    now = time.time()
    alive = {MY_ID}
    for pid in OTHERS:
        if now - last_seen.get(pid, 0) <= PEER_TIMEOUT:
            alive.add(pid)
    return alive

def _replication_targets():
    """Alvos da replicacao (W3): peers vistos ha pouco tempo. A janela e
    MAIOR que o timeout de morte de proposito - preferimos tentar replicar
    para um no possivelmente vivo (o timeout do REPLICATE resolve) a
    arriscar PULAR um backup vivo por causa de uma visao momentaneamente
    atrasada. Nos mortos ha muito tempo sao pulados (escrita rapida)."""
    now = time.time()
    return [pid for pid in OTHERS
            if now - last_seen.get(pid, 0) <= RECENT_WINDOW]

def _adopt_view(msg):
    """Regra de adocao de visao: EPOCA MAIOR SEMPRE VENCE.
    Resolve qualquer conflito de 'quem e o primario' apos eleicoes."""
    global epoch, primary_id
    m_epoch = msg.get('epoch', 0)
    m_primary = msg.get('primary_id')
    with state_lock:
        if m_epoch > epoch and m_primary is not None:
            if primary_id == MY_ID and m_primary != MY_ID:
                log(f"Epoca {m_epoch} > minha ({epoch}). Deixo de ser primario. "
                    f"Novo primario: {m_primary}")
            epoch = m_epoch
            primary_id = m_primary

# ----------------------------------------------------------------------------
# PING / Deteccao de falha / Eleicao
# ----------------------------------------------------------------------------

def _my_view():
    return {'id': MY_ID, 'epoch': epoch, 'primary_id': primary_id,
            'seq': seq, 'syncing': syncing}

def _election_check():
    """Executada a cada batida de coracao. Se o primario atual esta morto
    (ou ainda nao existe), o MENOR ID VIVO assume, incrementando a epoca."""
    global epoch, primary_id
    with state_lock:
        alive = _alive_ids()
        # Nao considere nos em sincronizacao como candidatos (estado defasado)
        candidates = sorted(alive)
        if primary_id in alive and primary_id is not None:
            return  # primario atual segue vivo: nada a fazer (lideranca "sticky")
        if syncing:
            return  # eu ainda nao tenho estado valido; nao posso reivindicar
        new_primary = candidates[0]
        if new_primary == MY_ID:
            old = primary_id
            epoch += 1
            primary_id = MY_ID
            counters['elections'] += 1
            log(f"*** ELEICAO: assumo como PRIMARIO (epoca {epoch}). "
                f"Primario anterior: {old}. Vivos: {sorted(alive)} ***")
            emit('ELECTION', new_primary=MY_ID, epoch=epoch,
                 old_primary=old, alive=sorted(alive))
        else:
            # O candidato vai reivindicar e propagar via PING; enquanto isso
            # aponto para ele provisoriamente (sem mexer na epoca).
            primary_id = new_primary

_known_dead = set()
_last_catchup = 0.0
_coup_strikes = 0   # observacoes seguidas de "primario atras de mim"

def _maybe_catchup(view):
    """Anti-entropia guiada pelos PINGs (que carregam epoca/seq/primario):
      - estou ATRASADO em relacao a um peer => busco snapshot;
      - estou A FRENTE do PRIMARIO ATUAL => minhas escritas extras vieram
        de uma epoca morta; adoto o estado autoritativo dele (rollback).
    O catchup NAO derruba o servico (diferente do _resync de recuperacao)."""
    global _last_catchup, _coup_strikes, epoch, primary_id
    with state_lock:
        if syncing or primary_id == MY_ID:
            return
        v_seq = view.get('seq', 0)
        # Histerese de +-1: durante uma escrita em voo e normal um no estar
        # exatamente 1 seq a frente/atras do outro por alguns milissegundos.
        behind = v_seq > seq + 1
        newer_epoch = view.get('epoch', 0) > epoch
        view_is_primary = (view.get('id') == view.get('primary_id')
                           and view.get('id') == primary_id)
        ahead_of_primary = view_is_primary and v_seq + 1 < seq

        # --- Caso extremo: PRIMARIO AMNESICO ---
        # O primario atual esta PERSISTENTEMENTE atras de mim na mesma epoca
        # (reiniciou tao rapido que ninguem detectou a queda e ainda por
        # cima nao conseguiu re-sincronizar). Como escritas sao serializadas,
        # um primario saudavel nunca fica atras de um backup. Depois de 3
        # observacoes seguidas, o MENOR backup vivo assume com epoca+1;
        # o amnesico vai ver a epoca maior, se rebaixar e re-adotar o estado.
        if ahead_of_primary and not (behind or newer_epoch):
            _coup_strikes += 1
            if _coup_strikes >= 3:
                _coup_strikes = 0
                alive = _alive_ids()
                candidates = sorted(i for i in alive if i != primary_id)
                if candidates and candidates[0] == MY_ID:
                    old = primary_id
                    epoch += 1
                    primary_id = MY_ID
                    counters['elections'] += 1
                    log(f"*** ELEICAO (primario {old} amnesico, seq dele="
                        f"{v_seq} < meu {seq}): assumo como PRIMARIO "
                        f"(epoca {epoch}) ***")
                    emit('ELECTION', new_primary=MY_ID, epoch=epoch,
                         old_primary=old, alive=sorted(alive),
                         reason='primario amnesico')
            return
        _coup_strikes = 0

        if not (behind or newer_epoch):
            return
        if time.time() - _last_catchup < 5.0:
            return
        _last_catchup = time.time()
        log(f"Divergencia notada via PING (meu seq={seq}/ep{epoch}, peer "
            f"{view.get('id')} tem {v_seq}/ep{view.get('epoch')}). Buscando snapshot...")
    threading.Thread(target=_catchup, daemon=True).start()

def _ping_peer(pid, url):
    try:
        r = requests.post(f'{url}/ping', json=_my_view(), timeout=PING_TIMEOUT)
        if r.status_code == 200:
            pong = r.json()
            last_seen[pid] = time.time()
            _adopt_view(pong)
            _maybe_catchup(pong)
            if pid in _known_dead:
                _known_dead.discard(pid)
                log(f"Peer {pid} VOLTOU a responder PING.")
                emit('PEER_BACK', peer=pid)
    except Exception:
        pass  # timeout => o peer continua com last_seen antigo

def _heartbeat_loop():
    """Thread: envia PING aos outros nos a cada HB_INTERVAL e detecta mortes.
    Os PINGs sao PARALELOS: um peer morto (timeout) nao atrasa a rodada,
    entao a visao de vivacidade dos peers vivos fica sempre fresca."""
    while True:
        time.sleep(HB_INTERVAL)
        if failed:
            continue  # em omissao eu tambem NAO envio pings (estou "mudo")
        threads = []
        for pid, url in OTHERS.items():
            t = threading.Thread(target=_ping_peer, args=(pid, url), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(PING_TIMEOUT + 0.3)
        # Deteccao de morte por timeout de PING
        now = time.time()
        for pid in OTHERS:
            dead = now - last_seen.get(pid, 0) > PEER_TIMEOUT
            if dead and pid not in _known_dead and last_seen.get(pid, 0) > 0:
                _known_dead.add(pid)
                log(f"!!! Peer {pid} NAO responde PING ha {PEER_TIMEOUT}s => MORTO !!!")
                emit('PEER_DEAD', peer=pid, detected_by=MY_ID)
        _election_check()

@app.route('/ping', methods=['POST'])
def ping():
    """Mensagem de controle PING (proposta). Tambem carrega a visao do
    remetente (epoca/primario), o que acelera a convergencia da eleicao."""
    if _omission_guard():
        return jsonify({'error': 'omission'}), 503
    msg = request.get_json(silent=True) or {}
    pid = msg.get('id')
    if pid in OTHERS:
        last_seen[pid] = time.time()
        _adopt_view(msg)
        _maybe_catchup(msg)
    return jsonify(_my_view())

# ----------------------------------------------------------------------------
# Escrita (Protocolo Primario-Backup - Figura 7.19 da proposta)
# ----------------------------------------------------------------------------

def _replicate_to_backups(payload):
    """W3/W4: primario manda a atualizacao aos backups possivelmente vivos
    e espera os ACKs. A replicacao e feita em paralelo (thread por backup)."""
    acks, misses = [], []
    targets = _replication_targets()
    emit('REPLICATE', seq=payload['seq'], resource=payload['resource'],
         to=targets)
    threads = []
    results = {}

    def _send(pid, url):
        try:
            r = requests.post(f'{url}/replicate', json=payload, timeout=REPL_TIMEOUT)
            results[pid] = (r.status_code == 200)
        except Exception:
            results[pid] = False

    for pid in targets:
        t = threading.Thread(target=_send, args=(pid, OTHERS[pid]), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(REPL_TIMEOUT + 0.5)

    for pid in targets:
        (acks if results.get(pid) else misses).append(pid)
    if misses:
        log(f"Backups sem ACK (provavelmente caidos): {misses}")
    return acks, misses

@app.route('/write', methods=['POST'])
def write():
    """Porta de entrada da escrita (W1). Qualquer no do Store aceita a
    requisicao do Cluster Sync - se nao for o primario, repassa (W2)."""
    global seq
    if _omission_guard():
        return jsonify({'error': 'omission'}), 503
    if syncing:
        return jsonify({'error': 'SYNCING', 'primary_id': primary_id}), 503

    req = request.get_json(silent=True) or {}
    req_id = req.get('req_id')
    res_name = req.get('resource')
    if not req_id or res_name not in resources:
        return jsonify({'error': 'requisicao invalida'}), 400

    # --- Nao sou o primario: repasso a requisicao (W2) ---
    if primary_id != MY_ID:
        if req.get('forwarded_by'):
            # Ja veio repassada e eu tambem nao sou o primario: visao em
            # transicao (eleicao em curso). Nao criamos loop de repasse;
            # o Cluster Sync vai retentar em instantes.
            return jsonify({'error': 'PRIMARY_IN_TRANSITION'}), 503
        if primary_id is None or primary_id not in _alive_ids():
            return jsonify({'error': 'PRIMARY_UNKNOWN'}), 503
        counters['forwards'] += 1
        emit('FORWARD', to=primary_id, resource=res_name,
             sync_id=req.get('sync_id'))
        log(f"Nao sou o primario. Repassando escrita {req_id[:8]} "
            f"({res_name}) ao primario {primary_id} (W2)")
        try:
            fwd = dict(req)
            fwd['forwarded_by'] = MY_ID
            r = requests.post(f'{PEERS[primary_id]}/write', json=fwd,
                              timeout=FORWARD_TIMEOUT)
            body = r.json()
            body['routed_via'] = MY_ID   # informa por onde a escrita entrou
            return jsonify(body), r.status_code
        except Exception as e:
            log(f"Falha ao repassar ao primario {primary_id}: {e}")
            return jsonify({'error': 'PRIMARY_UNREACHABLE',
                            'primary_id': primary_id}), 503

    # --- Sou o primario ---
    with state_lock:
        # Idempotencia: se o Sync reenviar a mesma req apos timeout,
        # devolvemos o resultado ja commitado (nada e aplicado 2x).
        if req_id in applied_reqs:
            cached = dict(applied_reqs[req_id])
            cached['dedup'] = True
            return jsonify(cached)
        if req_id in inflight_reqs:
            # A original ainda esta sendo replicada; o retry aguarda
            return jsonify({'error': 'IN_FLIGHT'}), 409
        inflight_reqs.add(req_id)

        # Figura 7.19: "o servidor primario executa a atualizacao em sua
        # copia LOCAL e, na sequencia, envia a atualizacao aos backups".
        # Aplicar ANTES de replicar garante que /snapshot e /ping nunca
        # anunciam um seq cujo estado ainda nao existe (evita corridas
        # entre replicacao e re-sincronizacao).
        seq += 1
        w_seq = seq
        old = resources[res_name]
        record = {
            'value': old['value'] + int(req.get('amount', 1)),
            'version': w_seq,
            'last_writer': req.get('client_id'),
            'last_sync': req.get('sync_id'),
            'last_ts': time.time(),
        }
        _apply_write(w_seq, req_id, res_name, record)
        counters['writes_committed'] += 1

    payload = {'seq': w_seq, 'req_id': req_id, 'resource': res_name,
               'record': record, 'epoch': epoch, 'primary_id': MY_ID}

    # W3/W4: replica para os backups e espera os ACKs
    acks, misses = _replicate_to_backups(payload)

    result = {'status': 'COMMITTED', 'seq': w_seq, 'resource': res_name,
              'value': record['value'], 'version': w_seq,
              'primary_id': MY_ID, 'epoch': epoch,
              'replicas_ack': acks, 'replicas_down': misses}

    # Atualiza o cache de deduplicacao com o resultado completo (com ACKs)
    with state_lock:
        applied_reqs[req_id] = result
        inflight_reqs.discard(req_id)

    log(f"COMMIT seq={w_seq} {res_name}={record['value']} "
        f"(cliente {record['last_writer']} via Sync {record['last_sync']}; "
        f"ACKs: {acks or 'nenhum backup vivo'})")
    emit('WRITE_COMMIT', seq=w_seq, resource=res_name, value=record['value'],
         client_id=record['last_writer'], sync_id=record['last_sync'],
         acks=acks, misses=misses)

    return jsonify(result)   # W5: reconhece a escrita concluida

@app.route('/replicate', methods=['POST'])
def replicate():
    """Backup recebendo atualizacao do primario (W3). Aplica e da ACK (W4)."""
    if _omission_guard():
        return jsonify({'error': 'omission'}), 503
    msg = request.get_json(silent=True) or {}
    _adopt_view(msg)
    m_seq = msg.get('seq', 0)

    with state_lock:
        if syncing:
            # Ainda buscando snapshot; nao posso aceitar increments soltos
            return jsonify({'error': 'SYNCING'}), 409
        if m_seq <= seq:
            return jsonify({'status': 'ACK', 'dup': True})  # ja aplicado
        if m_seq > seq + 1:
            # Buraco na sequencia: perdi escritas (estive fora). Busco o
            # snapshot em background sem parar de servir.
            log(f"GAP detectado (meu seq={seq}, recebi {m_seq}). Pedindo snapshot...")
            threading.Thread(target=_catchup, daemon=True).start()
            return jsonify({'error': 'GAP_RESYNC'}), 409
        _apply_write(m_seq, msg['req_id'], msg['resource'], msg['record'])
        counters['replicates_applied'] += 1

    emit('REPLICATE_ACK', to_primary=msg.get('primary_id'), seq=m_seq)
    return jsonify({'status': 'ACK', 'seq': m_seq})

# ----------------------------------------------------------------------------
# Snapshot / Re-sincronizacao (recuperacao de um no que caiu e voltou)
# ----------------------------------------------------------------------------

@app.route('/snapshot', methods=['GET'])
def snapshot():
    if _omission_guard():
        return jsonify({'error': 'omission'}), 503
    with state_lock:
        return jsonify({'seq': seq, 'epoch': epoch, 'primary_id': primary_id,
                        'resources': resources,
                        'applied_reqs': list(applied_reqs.items())[-500:],
                        'history': list(history)})

def _fetch_and_adopt():
    """Baixa o snapshot de outro no e adota o estado quando apropriado.

    REGRA DE ADOCAO (ordem lexicografica por (epoca, seq)):
      - epoca do snapshot MAIOR que a minha  => adoto o estado inteiro,
        inclusive "voltando atras" no seq: minhas escritas extras vieram
        de uma epoca morta (fui primario, cai, houve eleicao). A epoca
        maior prova que uma eleicao legitima aconteceu.
      - mesma epoca => adoto apenas se o seq for MAIOR que o meu.
        NUNCA regredimos dentro da mesma epoca: isso protege o cluster
        contra um primario que reinicia "amnesico" tao rapido que ninguem
        detectou a queda (sem deteccao nao ha eleicao nem epoca nova, e o
        estado dos backups e que e o verdadeiro - ele e que deve puxar).

    Retorna o id do no consultado com sucesso, ou None.
    """
    global seq, epoch, primary_id
    order = []
    if primary_id and primary_id != MY_ID:
        order.append(primary_id)
    order += [p for p in OTHERS if p not in order]
    for pid in order:
        try:
            r = requests.get(f'{PEERS[pid]}/snapshot', timeout=3.0)
            if r.status_code != 200:
                continue
            snap = r.json()
            s_epoch, s_seq = snap.get('epoch', 0), snap.get('seq', 0)
            with state_lock:
                adopted = (s_epoch > epoch) or (s_epoch == epoch and s_seq > seq)
                if adopted:
                    seq = snap['seq']
                    for k, v in snap['resources'].items():
                        if k in resources:
                            resources[k] = v
                    applied_reqs.clear()
                    applied_reqs.update(dict(snap.get('applied_reqs', [])))
                    history.clear()
                    for h in snap.get('history', []):
                        history.append(h)
                    if snap.get('epoch', 0) >= epoch:
                        epoch = snap['epoch']
                        if snap.get('primary_id'):
                            primary_id = snap['primary_id']
                    counters['resyncs'] += 1
            if adopted:
                log(f"Estado adotado do no {pid} (seq={seq}, epoca={epoch})")
                emit('RESYNC', source=pid, seq=seq)
            return pid
        except Exception:
            continue
    return None

def _resync():
    """Recuperacao completa (boot / pos-queda / pos-omissao): o no PARA de
    servir (estado SINCRONIZANDO), adota o estado do cluster e volta.
    Insiste algumas vezes antes de desistir - uma janela transitoria de
    rede nao pode fazer o no comecar a servir com estado vazio."""
    global syncing
    with state_lock:
        syncing = True
    src = None
    for attempt in range(4):
        src = _fetch_and_adopt()
        if src is not None:
            break
        time.sleep(1.5)
    with state_lock:
        syncing = False
    if src is None:
        log("Nenhum peer respondeu ao snapshot: iniciando com estado local (zerado).")

def _catchup():
    """Anti-entropia leve: adota estado mais novo SEM parar de servir
    (a flag syncing nao e alterada; replicates continuam aceitos)."""
    _fetch_and_adopt()

# ----------------------------------------------------------------------------
# Administracao / Injecao de falhas / Observabilidade
# ----------------------------------------------------------------------------

@app.route('/admin/fail', methods=['POST'])
def admin_fail():
    """Simula FALHA POR OMISSAO: o processo continua vivo, mas para de
    responder ao protocolo (write/replicate/ping/snapshot)."""
    global failed
    body = request.get_json(silent=True) or {}
    duration = float(body.get('duration', 0) or 0)
    failed = True
    log(f"### FALHA POR OMISSAO INJETADA (duracao: "
        f"{duration if duration else 'ate /admin/recover'}) ###")
    emit('STORE_FAIL', mode='omission', duration=duration)
    if duration > 0:
        def _auto_recover():
            time.sleep(duration)
            _do_recover()
        threading.Thread(target=_auto_recover, daemon=True).start()
    return jsonify({'status': 'failed', 'mode': 'omission', 'duration': duration})

@app.route('/admin/crash', methods=['POST'])
def admin_crash():
    """Simula FALHA POR QUEDA de verdade: mata o processo. O orquestrador
    (docker compose 'restart: always' ou o Kubernetes) vai reinicia-lo,
    e o no volta pelo caminho de recuperacao (snapshot/resync)."""
    log("### FALHA POR QUEDA INJETADA: encerrando o processo AGORA ###")
    emit('STORE_CRASH')
    def _die():
        time.sleep(0.3)
        os._exit(1)
    threading.Thread(target=_die, daemon=True).start()
    return jsonify({'status': 'crashing'})

def _do_recover():
    global failed
    if not failed:
        return
    failed = False
    log("### NO RECUPERADO da omissao. Re-sincronizando estado... ###")
    emit('STORE_RECOVER')
    # Ao voltar, posso ter perdido escritas => re-sincroniza por seguranca
    threading.Thread(target=_resync, daemon=True).start()

@app.route('/admin/recover', methods=['POST'])
def admin_recover():
    _do_recover()
    return jsonify({'status': 'recovering'})

@app.route('/health', methods=['GET'])
def health():
    """Liveness probe do Kubernetes. Responde 200 MESMO em omissao simulada:
    quem deve detectar a omissao e o PROTOCOLO (timeout de PING), nao o k8s.
    Em queda real (crash) o processo morre e ai sim o k8s reinicia o pod."""
    return jsonify({'ok': True})

@app.route('/status', methods=['GET'])
def status():
    """Visao completa do no para o Dashboard (canal administrativo)."""
    now = time.time()
    with state_lock:
        if failed:
            role = 'OMISSAO'
        elif syncing:
            role = 'SINCRONIZANDO'
        elif primary_id == MY_ID:
            role = 'PRIMARIO'
        else:
            role = 'BACKUP'
        return jsonify({
            'id': MY_ID, 'role': role, 'epoch': epoch, 'primary_id': primary_id,
            'seq': seq, 'failed': failed, 'syncing': syncing,
            'uptime': round(now - started_at, 1),
            'alive_peers': sorted(_alive_ids()),
            'peer_ages': {pid: round(now - last_seen.get(pid, 0), 1)
                          for pid in OTHERS},
            'resources': resources,
            'counters': counters,
            'history': list(history)[:8],
        })

# ----------------------------------------------------------------------------
# Inicializacao
# ----------------------------------------------------------------------------

def _startup():
    """Espera o cluster se formar, tenta recuperar estado existente e
    entao participa da primeira eleicao."""
    log(f"Iniciando Store {MY_ID} | peers: {OTHERS} | recursos: {RESOURCES}")
    emit('STORE_START')
    time.sleep(STARTUP_GRACE)
    _resync()          # se ja existe estado no cluster, adota; senao zera
    _election_check()  # define o primario inicial (menor ID vivo)
    log(f"Pronto. Papel inicial: "
        f"{'PRIMARIO' if primary_id == MY_ID else f'BACKUP (primario={primary_id})'}")

if __name__ == '__main__':
    threading.Thread(target=_event_sender, daemon=True).start()
    threading.Thread(target=_heartbeat_loop, daemon=True).start()
    threading.Thread(target=_startup, daemon=True).start()
    # threaded=True: atende PING/REPLICATE/WRITE simultaneamente
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
