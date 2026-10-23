"""
Maintenance Case API
=====================
POST /cases  -> creates a maintenance case, returns case_id + assigned_team
GET  /health -> health probe for docker healthcheck
GET  /cases/{case_id} -> retrieve a case

Simulates a ~200ms response time per the brief. Persists cases to Postgres
(maintenance_cases table) so the SDK scripts and reports can read them back.
"""
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(title="Oil & Gas Maintenance Case API", version="1.0")

# Prometheus metrics - scraped at GET /metrics, surfaced in Grafana
CASES_CREATED = Counter(
    "maintenance_cases_created_total",
    "Total maintenance cases created",
    ["severity", "assigned_team"],
)
CASE_LATENCY = Histogram(
    "maintenance_case_request_seconds",
    "Latency of POST /cases requests",
)

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    user=os.getenv("DB_USER", "kermit"),
    password=os.getenv("DB_PASSWORD", "kermit"),
    dbname=os.getenv("DB_NAME", "oilgas"),
)

# Simple deterministic team routing by anomaly type
TEAM_ROUTING = {
    "pressure_bar": "Pressure Integrity Team",
    "temp_celsius": "Thermal Systems Team",
    "flow_rate_m3h": "Flow Assurance Team",
    "vibration_g": "Mechanical Integrity Team",
    "valve_status": "Valve & Controls Team",
}
DEFAULT_TEAM = "Field Operations Team"


class CaseRequest(BaseModel):
    well_id: str
    anomaly_type: str
    severity: str
    timestamp: str


def get_conn():
    return psycopg2.connect(**DB)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/cases")
@CASE_LATENCY.time()
def create_case(req: CaseRequest):
    time.sleep(0.2)  # simulate ~200ms processing
    case_id = f"CASE-{uuid.uuid4().hex[:10].upper()}"
    assigned_team = TEAM_ROUTING.get(req.anomaly_type, DEFAULT_TEAM)
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO maintenance_cases
                  (case_id, well_id, anomaly_type, severity, assigned_team, created_ts)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (case_id, req.well_id, req.anomaly_type, req.severity,
                 assigned_team, req.timestamp),
            )
        conn.close()
    except Exception as e:
        # Surface DB failures so the pipeline's retry/alert logic can react
        raise HTTPException(status_code=503, detail=f"case persistence failed: {e}")

    CASES_CREATED.labels(severity=req.severity, assigned_team=assigned_team).inc()
    return {
        "case_id": case_id,
        "well_id": req.well_id,
        "anomaly_type": req.anomaly_type,
        "severity": req.severity,
        "assigned_team": assigned_team,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/cases/{case_id}")
def get_case(case_id: str):
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM maintenance_cases WHERE case_id = %s", (case_id,))
            row = cur.fetchone()
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))
    if not row:
        raise HTTPException(status_code=404, detail="case not found")
    return row
