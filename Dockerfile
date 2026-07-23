# ============================================================================
# TP3 - Imagem unica para todos os componentes Python do sistema.
# O comando de cada servico (store/sync/client/dashboard) e definido no
# docker-compose.yml ou nos manifests do Kubernetes.
# ============================================================================
FROM python:3.12-slim

WORKDIR /app

# Saida de log sem buffer (essencial para acompanhar os containers)
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY store_node.py sync_node.py client.py dashboard.py ./
COPY templates/ templates/

# Padrao: dashboard (sobrescrito por cada servico)
CMD ["python", "dashboard.py"]
