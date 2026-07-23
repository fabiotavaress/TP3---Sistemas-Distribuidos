# Trabalho Prático 3 — Replicação e Tolerância a Falhas

> Repositório: **https://github.com/fabiotavaress/TP3---Sistemas-Distribuidos**

Continuação direta do **TP2** (exclusão mútua distribuída — Alternativa 3): agora, quando um
nó do Cluster Sync entra na seção crítica, ele **não simula mais** o acesso ao recurso R com
um `sleep`. Ele **escreve de verdade** em um **Cluster Store com 3 réplicas**, que implementa
o **Protocolo 1 da proposta (Primário-Backup, Figura 7.19)** com **tolerância a falhas por
queda e por omissão** (Opção 2 da proposta), tudo **sem middleware** na parte nova — a
comunicação do Store é 100% HTTP puro implementado à mão.

O sistema roda inteiro em **Kubernetes** (kind, Docker Desktop ou EC2 com k3s) ou em
**Docker Compose**, com um **dashboard animado em tempo real** que mostra cada mensagem do
protocolo, permite **injetar falhas com um clique** e prova visualmente a **consistência das
réplicas**.

---

## 1. Arquitetura

```
                         CLUSTER STORE (replicação primário-backup, HTTP puro)
                  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
                  │   Store 1    │◄──┤   Store 2    ├──►│   Store 3    │
                  │ 👑 PRIMÁRIO  │   │    BACKUP    │   │    BACKUP    │
                  │ R1..R5       │   │ R1..R5       │   │ R1..R5       │
                  └──────▲───────┘   └──────▲───────┘   └──────▲───────┘
                         │    PING / REPLICATE / SNAPSHOT      │
                         └───────────────┬─────────────────────┘
                                         │  escrita HTTP (nó SORTEADO a cada
                                         │  seção crítica + timeout + retry)
        ┌──────────┬──────────┬──────────┴┬───────────┬──────────┐
        │  Sync 1  │  Sync 2  │  Sync 3   │  Sync 4   │  Sync 5  │   CLUSTER SYNC
        │ (fila F) │ (fila F) │ (fila F)  │ (fila F)  │ (fila F) │   (herança TP2)
        └────▲─────┴────▲─────┴────▲──────┴────▲──────┴────▲─────┘
             │          │  ACQUIRE / RELEASE (multicast FIFO)  │
        ═════╪══════════╪═════ RabbitMQ · tópico R_topic ══════╪═════   (herança TP2)
             │          │          │           │               │
        ┌────┴─────┬────┴─────┬────┴──────┬────┴──────┬────────┴─┐
        │ Cliente 1│ Cliente 2│ Cliente 3 │ Cliente 4 │ Cliente 5│   CLIENTES
        └──────────┴──────────┴───────────┴───────────┴──────────┘
          cada requisição sorteia um RECURSO (R1..R5) para escrever
```

| Componente | Qtd | Arquivo | Papel |
|---|---|---|---|
| Cliente | 5 | `client.py` | Conhece **apenas um** nó do Sync; faz 10–50 pedidos por lote; **sorteia o recurso (R1..R5) a cada requisição**; espera COMMITTED; dorme 1–5 s |
| Cluster Sync | 5 | `sync_node.py` | Exclusão mútua da **Alternativa 3** (TP2): fila F privada + multicast ordenado via `R_topic`. Na seção crítica faz a **escrita real** no Store |
| Cluster Store | 3 | `store_node.py` | Réplicas do recurso R. **Primário-backup** com PING, eleição por época, snapshot/resync e deduplicação por `req_id` |
| Dashboard | 1 | `dashboard.py` + `templates/index.html` | Painel animado (SSE) + painel de controle de falhas |
| RabbitMQ | 1 | — | **Herança do TP2** (o broker era requisito de lá). A parte nova do TP3 não usa middleware nenhum |

### Por que o RabbitMQ ainda aparece?

A proposta do TP3 diz *"Iremos usar o que foi feito no TP2"* — e o TP2 **exigia** um broker
pub/sub com o tópico `R_topic` para o multicast ordenado do Cluster Sync. Ele permanece
exatamente com esse papel herdado. Já **tudo que é novo no TP3** (Sync → Store e
Store ↔ Store: escrita, repasse, replicação, PING, snapshot) é **HTTP puro, sem
middleware**, implementado manualmente com timeouts e retentativas — atendendo ao *"Não há
utilização de middlewares neste TP"*.

---

## 2. O protocolo de replicação (Primário-Backup — Figura 7.19)

Cada passo do livro está mapeado no código (`store_node.py`):

| Passo | O que acontece aqui |
|---|---|
| **W1** — Requisição de escrita | O Sync **sorteia um nó qualquer** do Store e faz `POST /write` |
| **W2** — Repassa ao primário | Se o nó sorteado não é o primário, ele **encaminha** a requisição ao primário e devolve a resposta (campo `routed_via`) |
| **W3** — Diz aos backups | O primário incrementa o `seq` global, aplica localmente e envia `POST /replicate` **em paralelo** para todos os backups **vivos** |
| **W4** — Backups reconhecem | Cada backup aplica a atualização e responde ACK |
| **W5** — Reconhece a escrita | O primário responde `COMMITTED` (com `seq`, valor novo e lista de ACKs) ao Sync, que então faz o RELEASE e responde ao cliente |

**Leituras** podem ser servidas localmente por qualquer réplica (propriedade do protocolo
primário-backup) — é assim que o dashboard monta a grade de consistência, lendo o `/status`
de cada réplica individualmente.

### Detalhes que garantem a correção

- **`seq` global monotônico**: o primário numera cada escrita; a versão de cada recurso é o
  `seq` da última escrita nele. Réplicas com o mesmo `seq` são **idênticas por construção**.
- **Idempotência / deduplicação**: toda requisição carrega um `req_id` único. Se o Sync
  retenta após um timeout (a escrita pode ter sido aplicada e só a resposta se perdeu!), o
  primário reconhece o `req_id` e devolve o resultado já commitado **sem aplicar de novo**.
- **Detecção de buraco (gap)**: se um backup recebe `seq = 7` mas só viu até `5`, ele sabe
  que perdeu escritas (esteve fora do ar) e dispara re-sincronização por snapshot.
- **Época (epoch)**: cada eleição incrementa a época. Visões conflitantes de "quem é o
  primário" são resolvidas pela regra *época maior sempre vence* — um primário antigo que
  volta do crash se **rebaixa sozinho** ao ver uma época maior nos PINGs.
- **Nunca regredir na mesma época**: um nó só adota estado de outro se ele for
  estritamente mais novo — época maior (permite *rollback* legítimo pós-eleição) ou, na
  mesma época, `seq` maior. Isso protege contra o **primário amnésico**: se o orquestrador
  reinicia o primário *tão rápido* que ninguém detecta a queda (não há eleição nem época
  nova), o recém-nascido **puxa** o estado dos backups em vez de empurrar o dele (vazio).
- **Golpe contra primário amnésico**: se mesmo assim um primário ficar *persistentemente*
  atrás de um backup na mesma época (só acontece com falha dupla: primário reinicia
  amnésico **e** não consegue re-sincronizar), o menor backup vivo assume com época+1
  após 3 observações consecutivas — o amnésico se rebaixa e re-adota o estado bom.
- **Anti-entropia contínua**: os PINGs carregam `(época, seq)`; um nó que se percebe 2+
  escritas atrasado busca o snapshot sozinho, sem esperar a próxima replicação (com
  histerese de ±1, pois durante uma escrita em voo é normal diferir de 1).

---

## 3. Tolerância a falhas — Opção 2 (falha em elemento do Cluster Store)

Falhas simuladas: **queda** (processo morre) e **omissão** (processo vivo, mas não responde
— nem envia — mensagens do protocolo). Detecção: **mensagens de controle PING** trocadas
entre os 3 nós a cada 0,8 s (**em paralelo**, para um nó morto não atrasar a rodada); sem
resposta por 4 s ⇒ nó considerado morto. Todas as comunicações têm **timeout**.

| Caso da proposta | O que acontece | Como demonstrar no dashboard |
|---|---|---|
| **2.1 — Store falha SEM pedido** | Os outros nós detectam por timeout de PING e o marcam morto. Se era backup, nada muda para as escritas (o primário passa a replicar só para quem está vivo) | Botão **💥 Derrubar um BACKUP**. O nó fica 💀, as escritas seguem normais |
| **2.2 — Store falha COM pedido do Sync** | O Sync que o sorteou toma **timeout**, e **retenta em outro nó sorteado**. O `req_id` garante que a escrita não é duplicada | Botão **🔇 Omissão no PRIMÁRIO por 15s** (ou derrube qualquer nó com tráfego rodando). Aparece "TIMEOUT … retentando em outro nó" no log |
| **2.3 — Store falha COM permissão de escrita (o PRIMÁRIO)** | Backups detectam a morte ⇒ **eleição**: o menor ID vivo assume com época+1. A escrita em andamento estoura timeout no Sync e é retentada — agora atendida pelo novo primário. Quando o antigo volta, vê a época maior, vira **backup** e re-sincroniza | Botão **💥 Derrubar o PRIMÁRIO**. A coroa 👑 muda de nó, o log mostra "⚡ ELEIÇÃO", e o nó antigo volta como backup (k8s/compose o reinicia sozinho) |

> **Nota sobre o Crash no Docker Compose:** o `restart: always` do Compose é tão rápido
> (~1 s) que muitas vezes o nó renasce **antes** do timeout de PING (4 s) — ou seja, a
> queda nem chega a ser detectada e não há eleição: o nó só re-sincroniza e segue (o que
> também é uma demonstração válida de recuperação!). Para ver a **eleição** acontecer com
> clareza, use o botão de **Omissão no primário (15 s)** — ou, no Kubernetes,
> `kubectl -n tp3 delete pod store-0`, pois a recriação do pod leva mais que o timeout.

### Recuperação (self-healing)

1. O orquestrador (Kubernetes ou `restart: always` do Compose) **reinicia o processo** que caiu.
2. O nó volta em estado `SINCRONIZANDO`: pede um **snapshot** completo a outro nó (de
   preferência o primário), adota `seq`/época/estado e só então passa a servir.
3. Nós em omissão, ao serem revividos, também re-sincronizam por segurança.
4. A **grade de consistência** do dashboard volta a ficar toda verde — prova visual de que a
   réplica recuperada convergiu.

---

## 4. Pontos extras pedidos em sala

> *"A cada requisição, o cliente tem que escolher um recurso aleatório pra escrever."*

Implementado em dois níveis (os dois sorteios são independentes e visíveis no dashboard):

1. **Cliente → recurso aleatório**: a cada requisição o cliente sorteia **qual recurso**
   escrever (`random.choice` de R1..R5) — `client.py`, função `acquire_resource()`. O
   recurso sorteado viaja no pedido, aparece na fila F e na tag amarela do painel.
2. **Sync → réplica aleatória**: a cada entrada na seção crítica (e a cada retentativa!) o
   nó do Sync sorteia **qual elemento do Cluster Store** recebe a escrita — `sync_node.py`,
   função `access_store()`. É o *"podendo inclusive variar de elemento do Cluster Store a
   cada entrada na seção crítica"* da proposta.

---

## 5. Como rodar

### Opção A — Docker Compose (mais rápido)

Requisito: Docker Desktop rodando.

```bash
cd TP3
docker compose up -d --build
```

- Dashboard: **http://localhost:5000**
- RabbitMQ (opcional): http://localhost:15672 (admin / admin123)

Parar tudo: `docker compose down`

### Opção B — Kubernetes local

**B.1 — Docker Desktop com Kubernetes habilitado** (Settings → Kubernetes → Enable):

```powershell
cd TP3
.\scripts\deploy_docker_desktop.ps1
```

**B.2 — kind (Kubernetes in Docker):**

```bash
cd TP3
./scripts/deploy_kind.sh
```

- Dashboard: **http://localhost:30500**

> No Kubernetes do Docker Desktop (que roda em "modo kind"), o NodePort não aparece
> sozinho no localhost — o script já abre um `kubectl port-forward svc/dashboard
> 30500:5000` para você. No kind com o `kind-config.yaml` e no EC2/k3s o NodePort
> funciona direto.

### Opção C — Nuvem (EC2 + k3s) ✨

1. Lance uma EC2 **Ubuntu 22.04/24.04** (mínimo `t3.medium`).
2. No **Security Group**, libere entrada nas portas **22**, **30500** e (opcional) **31672**.
3. Clone o repositório na instância e rode o script:

```bash
ssh -i sua-chave.pem ubuntu@<IP-PUBLICO>
git clone https://github.com/fabiotavaress/TP3---Sistemas-Distribuidos.git TP3
cd TP3 && chmod +x scripts/*.sh && ./scripts/deploy_ec2_k3s.sh
```

*(Alternativa sem git: `scp -i sua-chave.pem -r TP3 ubuntu@<IP-PUBLICO>:~/`)*

4. Abra **http://\<IP-PUBLICO\>:30500** — a sala inteira pode acessar e clicar nos clientes.

> Alternativa sem Kubernetes na EC2: instale o Docker e rode a Opção A; o dashboard fica na
> porta 5000 (libere-a no Security Group).

### Comandos úteis no Kubernetes

```bash
kubectl -n tp3 get pods -o wide            # ver os 15 pods
kubectl -n tp3 logs -f store-0             # acompanhar um nó do Store
kubectl -n tp3 logs -f sync-0              # acompanhar um nó do Sync
kubectl -n tp3 delete pod store-0          # FALHA POR QUEDA "raiz" (kubectl!)
kubectl -n tp3 delete -f k8s/              # desmontar tudo
```

`kubectl delete pod store-0` é a demonstração de queda mais convincente: o Kubernetes recria
o pod sozinho e dá para assistir, no dashboard, a detecção por PING → eleição → resync.

---

## 6. Como funciona cada arquivo

### `store_node.py` — o coração do TP3
- **Threads**: servidor Flask (endpoints do protocolo) + loop de PING + emissor de telemetria.
- **Endpoints do protocolo**: `/write` (W1/W2/W5), `/replicate` (W3/W4), `/ping`
  (detecção de falha + difusão da visão época/primário), `/snapshot` (recuperação).
- **Endpoints administrativos**: `/status` (visão completa para o dashboard), `/health`
  (liveness probe do k8s), `/admin/fail` (omissão), `/admin/crash` (queda real —
  `os._exit(1)`), `/admin/recover`.
- **Eleição**: menor ID vivo assume ao detectar morte do primário; época+1; liderança é
  "sticky" (um novo nó menor que entra depois **não** rouba a liderança — só há eleição
  quando o primário atual morre).
- Na **omissão**, o `/health` continua respondendo **de propósito**: quem deve detectar a
  falha é o protocolo (timeout de PING), não o Kubernetes — na **queda**, aí sim o k8s
  reinicia o pod.

### `sync_node.py` — herdado do TP2, com a seção crítica real
- Mesma Alternativa 3: `ACQUIRE`/`RELEASE` no fanout `R_topic`, fila F privada ordenada por
  timestamp, thread RPC separada para nunca perder pedidos de clientes.
- `access_store()`: sorteia réplica → `POST /write` com timeout 6 s → se falhar, sorteia
  outra e retenta (até 8×). Responde ao cliente com o resultado **real** (valor novo, `seq`,
  primário que atendeu, ACKs das réplicas).

### `client.py` — herdado do TP2, com o sorteio de recurso
- Lotes de 10–50 requisições, espera de 1–5 s entre elas (requisitos do TP2), e em modo
  container roda em loop contínuo para a demonstração nunca parar.

### `dashboard.py` + `templates/index.html`
- Backend: consome a telemetria do Sync/clientes via RabbitMQ, recebe eventos instantâneos
  dos Stores via `POST /event`, faz **poll** do `/status` das 3 réplicas (0,7 s) e manda
  tudo por **SSE** ao navegador. Também é o proxy dos botões de falha.
- Frontend (100% self-contained, sem CDN — funciona offline): topologia animada em canvas
  com partículas para **cada mensagem do protocolo** (ACQUIRE, escrita, repasse W2,
  replicação W3, ACKs W4, COMMITTED), coroa 👑 no primário, 💀/🔇 em nós mortos/mudos, fila
  F ao vivo, terminal de eventos, **grade de consistência versão·valor por réplica** e
  botões de cenário (2.1 / 2.2 / 2.3).

---

## 7. Estrutura de pastas

```
TP3/
├── Proposta.pdf              # enunciado
├── README.md                 # este arquivo
├── store_node.py             # Cluster Store (novo)
├── sync_node.py              # Cluster Sync (TP2 + seção crítica real)
├── client.py                 # Cliente (TP2 + recurso aleatório)
├── dashboard.py              # backend do painel
├── templates/index.html      # frontend animado
├── requirements.txt
├── Dockerfile                # imagem única (comando muda por serviço)
├── docker-compose.yml        # stack completo local
├── k8s/                      # manifests Kubernetes
│   ├── 00-namespace.yaml
│   ├── 01-rabbitmq.yaml      # Deployment + Services
│   ├── 02-store.yaml         # StatefulSet 3 réplicas + Service headless
│   ├── 03-sync.yaml          # StatefulSet 5 réplicas
│   ├── 04-clients.yaml       # StatefulSet 5 réplicas
│   └── 05-dashboard.yaml     # Deployment + NodePort 30500
├── scripts/
│   ├── build_image.(ps1|sh)
│   ├── deploy_docker_desktop.ps1
│   ├── deploy_kind.sh
│   ├── deploy_ec2_k3s.sh
│   └── kind-config.yaml
├── TP03_Apresentacao.pptx    # slides da apresentação (estética do TP2)
└── slides/                   # gerador dos slides (pptxgenjs)
    ├── gen.js
    └── icons.js
```

**Por que StatefulSets?** Cada réplica precisa de **identidade estável** (Store 1/2/3,
Sync 1..5, C1..C5) e de **DNS individual** (`store-0.store`, …) para o protocolo
ponto-a-ponto — exatamente o que StatefulSet + Service headless dão. O código deriva o ID
do ordinal do hostname (`store-0` ⇒ ID 1). Nenhum nó do Store usa volume persistente **de
propósito**: queremos que a recuperação seja pelo **protocolo** (snapshot dos vivos), não
pelo disco.

---

## 8. Roteiro de demonstração sugerido (5 min)

1. **Sistema saudável** — mostre as partículas: pedido amarelo → seção crítica verde →
   escrita ciano no Store sorteado → (se caiu num backup) repasse W2 → replicação azul W3 →
   ACKs verdes W4 → COMMITTED roxo voltando ao cliente. Grade toda verde.
2. **Recurso aleatório (ponto extra)** — aponte no log: cada pedido sai com um R diferente;
   cada escrita entra por um Store diferente.
3. **Caso 2.1** — derrube um backup. PING detecta (💀), escritas seguem sem ele. Reviva:
   `SINCRONIZANDO` → snapshot → verde de novo.
4. **Caso 2.2** — omissão no primário com tráfego: o Sync toma timeout e retenta noutro nó;
   contador de retentativas sobe; nenhuma escrita se perde nem duplica (dedup por `req_id`).
5. **Caso 2.3** — derrube o primário: eleição em ~5 s (⚡ no log, coroa muda), escrita em
   voo é retentada e commitada pelo novo primário. O antigo volta como backup e re-sincroniza.
6. **Golpe final** — `kubectl -n tp3 delete pod store-0`: o k8s recria o pod e o protocolo
   faz o resto. Grade verde no final = **réplicas consistentes**.

---

## 9. Requisitos da proposta ✔

- [x] Estratégia de replicação implementada (**Protocolo 1 — primário-backup**)
- [x] Tolerância a falhas por **queda e omissão** (Opção 2: Store, casos 1.1/1.2/1.3)
- [x] **Sem middleware** na parte nova (HTTP puro; broker é herança obrigatória do TP2)
- [x] **Timeouts** em todas as comunicações + **PINGs periódicos** de controle
- [x] 5 clientes / 5 Sync / 3 Store, cada cliente conhece só o seu nó do Sync (TP2)
- [x] Cliente com ID único + timestamp; 10–50 acessos; espera 1–5 s (TP2)
- [x] Sync pode acessar **qualquer** elemento do Store, **variando a cada seção crítica**
- [x] **Extra:** cliente sorteia o **recurso** a cada requisição
- [x] Não é solução multicore: são 15 processos independentes em containers/pods separados
