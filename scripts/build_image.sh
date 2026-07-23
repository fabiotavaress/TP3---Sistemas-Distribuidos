#!/usr/bin/env bash
# Constroi a imagem unica do TP3 (Linux / macOS / Git Bash)
set -euo pipefail
cd "$(dirname "$0")/.."
docker build -t tp3-app:latest .
echo "Imagem tp3-app:latest construida com sucesso."
