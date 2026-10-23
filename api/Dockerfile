# Maintenance Case API - FastAPI + Uvicorn
# Built by docker-compose (build: ./api). Exposes POST /cases, GET /health,
# GET /cases/{id}, and GET /metrics for Prometheus.
FROM python:3.11-slim

WORKDIR /app

# psycopg2-binary needs no build deps, but keep image lean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
