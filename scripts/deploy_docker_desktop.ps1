# ============================================================================
# Deploy do TP3 no Kubernetes embutido do Docker Desktop (Windows).
# Requisito: Docker Desktop com "Enable Kubernetes" ativado em Settings.
# Uso:  .\scripts\deploy_docker_desktop.ps1
# Depois: abra http://localhost:30500
# ============================================================================
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "==> 1/3 Construindo a imagem..." -ForegroundColor Cyan
docker build -t tp3-app:latest .

Write-Host "==> 2/3 Selecionando o contexto docker-desktop..." -ForegroundColor Cyan
kubectl config use-context docker-desktop

Write-Host "==> 3/3 Aplicando os manifests..." -ForegroundColor Cyan
kubectl apply -f k8s/

Write-Host "`nAguardando os pods ficarem prontos..." -ForegroundColor Cyan
kubectl -n tp3 wait --for=condition=Ready pod --all --timeout=300s
kubectl -n tp3 get pods -o wide

Write-Host "`n=========================================================="
Write-Host " Dashboard:  http://localhost:30500" -ForegroundColor Green
Write-Host " RabbitMQ:   http://localhost:31672  (admin / admin123)"
Write-Host "=========================================================="
