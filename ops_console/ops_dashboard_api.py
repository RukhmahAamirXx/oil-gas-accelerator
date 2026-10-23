#!/usr/bin/env python3
"""
ops_dashboard_api.py — read-only API for the Oil & Gas Operations Console.
=========================================================================
A small Flask service that serves live monitoring data from Postgres to the
operations console UI (ops_console.html). READ-ONLY — it only runs SELECTs;
it never writes, so it cannot disturb the pipelines or the maintenance system.

This sits ALONGSIDE the required deliverables (Grafana, maintenance API,
Mailhog). It is a unified operator view, not a replacement for any of them.

Endpoints (all GET, JSON, CORS-enabled for the local UI):
  /api/stats              KPI cards: critical_24h, cases_today, wells_in_alarm,
                          anomaly_rate_per_min, warn_24h
  /api/cases?limit=50     recent maintenance cases (newest first)
  /api/alerts?limit=50    recent CRITICAL anomaly events (the live alert feed)
  /api/anomalies_by_well  per-well WARN/CRITICAL counts (last 24h)
  /api/severity_split     WARN vs CRITICAL counts (last 24h)
  /api/rate_series        anomalies/min for the last ~3h (sparkline/line)
  /api/health             liveness

Run:  python ops_dashboard_api.py            # serves on http://localhost:8050
Deps: flask, flask-cors, psycopg2-binary
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
import psycopg2.extras

PG = {"host": "localhost", "port": 5434, "dbname": "oilgas", "user": "kermit", "password": "kermit"}

app = Flask(__name__)
CORS(app)


def q(sql, params=None, one=False):
    conn = psycopg2.connect(**PG)
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        return (rows[0] if rows else None) if one else rows
    finally:
        conn.close()


@app.route("/api/health")
def health():
    try:
        q("SELECT 1", one=True)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 500


@app.route("/api/stats")
def stats():
    crit = q("SELECT COUNT(*) AS n FROM anomaly_events WHERE severity='CRITICAL' "
             "AND event_ts >= NOW() - INTERVAL '24 hours'", one=True)["n"]
    warn = q("SELECT COUNT(*) AS n FROM anomaly_events WHERE severity='WARN' "
             "AND event_ts >= NOW() - INTERVAL '24 hours'", one=True)["n"]
    cases = q("SELECT COUNT(*) AS n FROM maintenance_cases "
              "WHERE recorded_at::date = CURRENT_DATE", one=True)["n"]
    wells = q("SELECT COUNT(DISTINCT well_id) AS n FROM anomaly_events "
              "WHERE severity='CRITICAL' AND event_ts >= NOW() - INTERVAL '10 minutes'",
              one=True)["n"]
    rate = q("SELECT COUNT(*)::float / 5.0 AS r FROM anomaly_events "
             "WHERE event_ts >= NOW() - INTERVAL '5 minutes'", one=True)["r"] or 0
    return jsonify({
        "critical_24h": crit,
        "warn_24h": warn,
        "cases_today": cases,
        "wells_in_alarm": wells,
        "anomaly_rate_per_min": round(rate, 1),
    })


@app.route("/api/cases")
def cases():
    limit = min(int(request.args.get("limit", 50)), 200)
    rows = q("SELECT case_id, well_id, anomaly_type, severity, assigned_team, recorded_at "
             "FROM maintenance_cases ORDER BY recorded_at DESC LIMIT %s", (limit,))
    for r in rows:
        r["recorded_at"] = r["recorded_at"].isoformat() if r["recorded_at"] else None
    return jsonify(rows)


@app.route("/api/alerts")
def alerts():
    limit = min(int(request.args.get("limit", 50)), 200)
    rows = q("SELECT well_id, field_name, metric, value, severity, threshold_type, event_ts "
             "FROM anomaly_events WHERE severity='CRITICAL' "
             "ORDER BY event_ts DESC LIMIT %s", (limit,))
    for r in rows:
        r["event_ts"] = r["event_ts"].isoformat() if r["event_ts"] else None
        if r["value"] is not None:
            r["value"] = float(r["value"])
    return jsonify(rows)


@app.route("/api/anomalies_by_well")
def by_well():
    rows = q("SELECT well_id, "
             "COUNT(*) FILTER (WHERE severity='WARN') AS warn, "
             "COUNT(*) FILTER (WHERE severity='CRITICAL') AS critical "
             "FROM anomaly_events WHERE event_ts >= NOW() - INTERVAL '24 hours' "
             "GROUP BY well_id ORDER BY well_id")
    return jsonify(rows)


@app.route("/api/severity_split")
def severity_split():
    rows = q("SELECT severity, COUNT(*) AS n FROM anomaly_events "
             "WHERE event_ts >= NOW() - INTERVAL '24 hours' "
             "AND severity IN ('WARN','CRITICAL') GROUP BY severity")
    return jsonify({r["severity"]: r["n"] for r in rows})


@app.route("/api/rate_series")
def rate_series():
    rows = q("SELECT date_trunc('minute', event_ts) AS t, COUNT(*) AS n "
             "FROM anomaly_events WHERE event_ts >= NOW() - INTERVAL '3 hours' "
             "GROUP BY t ORDER BY t")
    return jsonify([{"t": r["t"].isoformat(), "n": r["n"]} for r in rows])


if __name__ == "__main__":
    print("Ops Console API on http://localhost:8050  (read-only)")
    app.run(host="0.0.0.0", port=8050, debug=False)
