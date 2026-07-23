# TP3 — Replicação e Tolerância a Falhas

> **Repositório:** https://github.com/fabiotavaress/TP3---Sistemas-Distribuidos

Sistemas Distribuídos · continuação do TP2. Roda em **Kubernetes** e na **nuvem AWS (EC2)**.

---

## O que foi feito

No TP2, quando um nó do **Cluster Sync** entrava na seção crítica, ele apenas *simulava*
o acesso ao recurso R com um `sleep`. No **TP3 isso virou real**: agora existe um
**Cluster Store** com **3 réplicas** que guardam o recurso R de verdade, usando o
**protocolo primário-backup** (Figura 7.19 da proposta) — **sem middleware**, tudo em
HTTP puro feito à mão.

**Como funciona uma escrita:**
1. Na seção crítica, o nó do Sync **sorteia uma réplica** do Store e envia a escrita.
2. Se a réplica sorteada não é a primária, ela **repassa ao primário**.
3. O primário aplica localmente e **replica para os backups**, que confirmam (ACK).
4. O primário responde `COMMITTED`; o Sync libera e devolve o valor ao cliente.

**Extra pedido em sala:** a cada requisição o **cliente sorteia o recurso** (R1–R5), e a
cada acesso o Sync **sorteia a réplica** do Store.

**Tolerância a falhas (queda e omissão de nós do Store):**
- Os 3 nós trocam **PINGs** periódicos; sem resposta no tempo limite → nó considerado morto.
- **Backup cai (2.1):** as escritas seguem sem ele; ao voltar, ele **re-sincroniza** por snapshot.
- **Falha com pedido em andamento (2.2):** o Sync toma timeout e **retenta em outra réplica**;
  a deduplicação por `req_id` impede escrita dupla.
- **Primário cai (2.3):** os backups fazem uma **eleição** (o menor ID vivo assume); a escrita
  é retentada no novo primário e o antigo volta como backup.

**Arquitetura:** 5 Clientes → 5 nós Cluster Sync (exclusão mútua do TP2, via RabbitMQ) →
3 nós Cluster Store (primário-backup, HTTP puro). Um **dashboard web** mostra tudo ao vivo.

---

## Rodar na EC2 com Kubernetes

**1. Na AWS:** crie uma instância **EC2 Ubuntu** (mínimo `t3.medium`) e, no **Security Group**,
libere as portas de entrada **22** (SSH) e **30500** (dashboard).

**2. Conecte e faça o deploy** (um script instala Docker + k3s, faz o build e sobe tudo):

```bash
ssh -i sua-chave.pem ubuntu@SEU_IP_PUBLICO

git clone https://github.com/fabiotavaress/TP3---Sistemas-Distribuidos.git tp3
cd tp3
chmod +x scripts/*.sh
./scripts/deploy_ec2_k3s.sh
```

**3. Abra no navegador:**  `http://SEU_IP_PUBLICO:30500`

Pronto. São **15 pods** no total (RabbitMQ + 3 Store + 5 Sync + 5 Cliente + Dashboard).

### Comandos do dia a dia (Kubernetes)

```bash
sudo k3s kubectl -n tp3 get pods                 # ver os 15 pods
sudo k3s kubectl -n tp3 logs -f store-0          # acompanhar um nó do Store
sudo k3s kubectl -n tp3 delete pod store-0       # DERRUBAR um nó (demonstra a falha)
sudo k3s kubectl -n tp3 delete -f k8s/           # desmontar tudo
```

> `delete pod store-0` é a melhor demonstração: o Kubernetes recria o pod sozinho e, no
> dashboard, dá pra ver a detecção por PING → eleição → re-sincronização. Para forçar a
> **eleição** com clareza, use o botão **"Omissão no primário (15s)"** do próprio dashboard.

---

## Rodar no PC (opcional, sem Kubernetes)

Precisa de Docker. Na pasta do projeto:

```bash
docker compose up -d --build     # sobe os 15 containers
# dashboard em http://localhost:5000
docker compose down              # encerra
```

---

## Estrutura

```
store_node.py        Cluster Store (novo) — primário-backup, PING, eleição, snapshot
sync_node.py         Cluster Sync (TP2) — agora escreve de verdade no Store
client.py            Cliente (TP2) — sorteia o recurso a cada requisição
dashboard.py         Painel web em tempo real
k8s/                 Manifests do Kubernetes (StatefulSets)
scripts/             deploy_ec2_k3s.sh, deploy_kind.sh, etc.
TP03_Apresentacao.pptx   Slides da apresentação
```
