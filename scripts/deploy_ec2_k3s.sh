#!/usr/bin/env bash
# ============================================================================
# Deploy do TP3 em uma instancia EC2 (Ubuntu 22.04/24.04) usando k3s,
# uma distribuicao leve de Kubernetes perfeita para uma unica VM.
#
# PASSO A PASSO NA AWS (uma vez):
#   1. Lance uma instancia EC2 Ubuntu (t3.medium ou maior recomendado).
#   2. No Security Group, libere as portas de ENTRADA:
#        22 (SSH), 30500 (dashboard) e opcionalmente 31672 (RabbitMQ mgmt).
#   3. Copie a pasta TP3 para a instancia:
#        scp -i sua-chave.pem -r TP3 ubuntu@<IP-PUBLICO>:~/
#   4. Conecte e rode este script:
#        ssh -i sua-chave.pem ubuntu@<IP-PUBLICO>
#        cd TP3 && chmod +x scripts/*.sh && ./scripts/deploy_ec2_k3s.sh
#   5. Abra no navegador:  http://<IP-PUBLICO>:30500
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> 1/5 Instalando Docker (se necessario)..."
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
fi

echo "==> 2/5 Instalando k3s (se necessario)..."
if ! command -v k3s >/dev/null 2>&1; then
  curl -sfL https://get.k3s.io | sudo sh -
fi
mkdir -p ~/.kube
sudo cp /etc/rancher/k3s/k3s.yaml ~/.kube/config
sudo chown "$USER" ~/.kube/config
export KUBECONFIG=~/.kube/config

echo "==> 3/5 Construindo a imagem..."
sudo docker build -t tp3-app:latest .

echo "==> 4/5 Importando a imagem para o containerd do k3s..."
sudo docker save tp3-app:latest | sudo k3s ctr images import -

echo "==> 5/5 Aplicando os manifests..."
sudo k3s kubectl apply -f k8s/

echo ""
echo "Aguardando os pods ficarem prontos (pode levar ~2 min)..."
sudo k3s kubectl -n tp3 wait --for=condition=Ready pod --all --timeout=420s || true
sudo k3s kubectl -n tp3 get pods -o wide

IP_PUB=$(curl -s --max-time 3 http://169.254.169.254/latest/meta-data/public-ipv4 || echo "<IP-PUBLICO>")
echo ""
echo "=========================================================="
echo " Dashboard:  http://${IP_PUB}:30500"
echo " RabbitMQ:   http://${IP_PUB}:31672  (admin / admin123)"
echo "=========================================================="
