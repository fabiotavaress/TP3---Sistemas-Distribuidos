#!/usr/bin/env bash
# ============================================================================
# Deploy do TP3 em um cluster kind (Kubernetes in Docker) local.
# Requisitos: docker, kind, kubectl
# Uso: ./scripts/deploy_kind.sh
# Depois: abra http://localhost:30500
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> 1/4 Construindo a imagem..."
docker build -t tp3-app:latest .

echo "==> 2/4 Criando o cluster kind 'tp3' (se nao existir)..."
if ! kind get clusters 2>/dev/null | grep -q '^tp3$'; then
  kind create cluster --config scripts/kind-config.yaml
fi

echo "==> 3/4 Carregando a imagem para dentro do cluster..."
kind load docker-image tp3-app:latest --name tp3

echo "==> 4/4 Aplicando os manifests..."
kubectl apply -f k8s/

echo ""
echo "Aguardando os pods ficarem prontos..."
kubectl -n tp3 wait --for=condition=Ready pod --all --timeout=300s || true
kubectl -n tp3 get pods -o wide
echo ""
echo "=========================================================="
echo " Dashboard:  http://localhost:30500"
echo " RabbitMQ:   http://localhost:31672  (admin / admin123)"
echo "=========================================================="
