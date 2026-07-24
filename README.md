# TP3 — Replicação e Tolerância a Falhas

> **Repositório:** https://github.com/fabiotavaress/TP3---Sistemas-Distribuidos
> Sistemas Distribuídos · continuação do TP2 · roda em **Kubernetes** na **AWS (EC2)**.

---

## 1. O que nós fizemos

No **TP2**, quando um nó do **Cluster Sync** ganhava a vez (seção crítica), ele só *fingia*
acessar o recurso R com um `sleep`. No **TP3 esse acesso virou real**: criamos um
**Cluster Store** com **3 réplicas** que guardam o recurso R de verdade, usando o
**protocolo primário-backup** (Figura 7.19 da proposta), **sem nenhum middleware** — toda a
comunicação do Store é HTTP puro, feito à mão.

**Como uma escrita acontece:**
1. O nó do Sync, na seção crítica, **sorteia uma das 3 réplicas** e manda a escrita.
2. Se a réplica sorteada não é a **primária**, ela **repassa para a primária**.
3. A primária grava e **replica para os backups**, que confirmam (ACK).
4. A primária responde **COMMITTED** e o cliente recebe o valor gravado.

**Extra pedido em sala:** a cada requisição o **cliente sorteia o recurso** (R1–R5) e a cada
acesso o Sync **sorteia a réplica** — nada é fixo.

**Tolerância a falhas (queda e omissão de nós do Store):** os 3 nós trocam **PINGs**; se um
para de responder por 4s, é considerado morto.
- **Backup cai:** as escritas continuam sem ele; ao voltar, ele **re-sincroniza** o estado.
- **Primário cai:** os backups fazem uma **eleição** (o menor ID vivo assume) e as escritas
  seguem no novo primário — **nada se perde** (retentativa + deduplicação por `req_id`).

**Infra:** os 5 Clientes, 5 Sync, 3 Store, o RabbitMQ e o Dashboard rodam como **pods no
Kubernetes** (k3s) numa instância **EC2 da AWS**. Se um pod cai, o próprio Kubernetes o
recria (**self-healing**). Um **dashboard web** mostra tudo ao vivo.

---

## 2. Roteiro para apresentação

### Antes de começar (ligar)

No terminal da EC2 (**Connect → EC2 Instance Connect**):

```bash
sudo k3s kubectl -n tp3 rollout restart statefulset/store statefulset/sync      # reseta limpo
sudo k3s kubectl -n tp3 port-forward --address 0.0.0.0 svc/dashboard 5000:5000  # deixa rodando
```
Abrir no navegador: **http://SEU_IP_PUBLICO:5000** *(o IP público está no console EC2)*

> Não há tráfego automático: os pedidos só aparecem quando alguém aperta **Rajada de 5
> pedidos** (ou clica num cliente) — a turma toda pode participar pelo navegador.

### Na hora — passo a passo

**PASSO 1 — apresentar a tela** *(só apontando)*
> **FALE:** "Embaixo estão os 5 **clientes**; no meio o **Cluster Sync** (5 nós) com o
> RabbitMQ, que é o que faz a exclusão mútua do TP2; e em cima o **Cluster Store**, com 3
> réplicas do recurso R — a que está com a coroa 👑 é a **primária**."

**PASSO 2 — provar que é Kubernetes** *(no terminal)*
```bash
sudo k3s kubectl -n tp3 get pods
```
> **FALE:** "Todo o sistema roda no **Kubernetes**: cada linha aqui é um pod — os 3 stores,
> os 5 syncs, o broker e o dashboard, todos gerenciados pelo k8s."

**PASSO 3 — fluxo normal** *(aperte **⚡ Rajada de 5 pedidos**)*
> **FALE:** "Cada bolinha é uma escrita: o cliente pede, um nó entra na seção crítica,
> grava numa réplica **sorteada**, a primária replica pros backups e responde COMMITTED.
> Repare na grade da direita: fica toda verde, ou seja, **as 3 réplicas ficam idênticas**."

**PASSO 4 — falha de um backup (caso 2.1)** *(aperte **💥 Derrubar um BACKUP**, depois **Rajada**)*
> **FALE:** "Derrubei um backup. Aperto Rajada de novo e repare: as **escritas continuam
> normalmente** — perder um backup não para o sistema."

**PASSO 5 — falha do primário (caso 2.3)** *(aperte **💥 Derrubar o primário**, espere ~5s, depois **Rajada**)*
> **FALE:** "Agora derrubei a **primária**. Em uns 5 segundos os outros percebem pelo PING
> e fazem uma **eleição** — olhem a coroa mudar de nó. Aperto Rajada e as escritas seguem
> no novo primário: **nada se perde**."

**PASSO 6 — recuperação** *(aperte **❤️ Reviver todos**)*
> **FALE:** "Os nós que caíram **voltam e re-sincronizam** o estado sozinhos; a grade volta
> ao verde — tudo consistente de novo."

**PASSO 7 — self-healing do Kubernetes** *(no terminal)*
```bash
sudo k3s kubectl -n tp3 delete pod store-0
sudo k3s kubectl -n tp3 get pods
```
> **FALE:** "Pra fechar: apago um pod na marra, e o **próprio Kubernetes recria ele em
> segundos**, sem eu fazer nada — é o self-healing."

### Resetar (recomeçar do zero, a qualquer momento)
```bash
sudo k3s kubectl -n tp3 rollout restart statefulset/store statefulset/sync
```

### No fim
Parar a instância na AWS: **Instance state → Stop** (pra não gastar).

---

<sub>Primeira instalação numa EC2 nova: `git clone` do repositório, depois
`cd tp3 && chmod +x scripts/*.sh && ./scripts/deploy_ec2_k3s.sh` (instala Docker + k3s e
sobe tudo). Rodar local sem Kubernetes: `docker compose up -d --build` (dashboard em
`http://localhost:5000`).</sub>
