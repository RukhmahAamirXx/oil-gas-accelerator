#!/usr/bin/env python3
"""
anomaly_recorder.py  - KSEA Oil & Gas anomaly recorder (Postgres)
==================================================================
Consumes normalised telemetry from Kafka sensors.normalised, classifies each
reading against thresholds.yaml, and records WARN/CRITICAL readings to the
Postgres anomaly_events table.

WHY A SCRIPT (not the JDBC Producer stage): the StreamSets JDBC Producer hit
repeated stage-specific issues (type coercion, varchar overflow, and a
parameter-binding error 'No value specified for parameter 7' that persisted even
with all fields present). psycopg2 gives full control of the INSERT and reuses
the proven pattern from audit_run_logger.py / mongo_archiver.py. The anomaly
DETECTION + classification still happens in the pipeline; this records the
results durably. (The pipeline's active in-canvas job remains classify -> route
-> CRITICAL alert dispatch.)

anomaly_events columns:
  well_id, field_name, event_ts (timestamptz), metric, value (numeric),
  severity, threshold_type, detected_at (default now())

Classification mirrors the pipeline's JS classifier (thresholds.yaml).

Usage:
  python anomaly_recorder.py                 # continuous, new events
  python anomaly_recorder.py --from-beginning
  python anomaly_recorder.py --once
  python anomaly_recorder.py --dry-run

Deps: kafka-python, psycopg2-binary
"""
import argparse
import json
import datetime as dt

# Thresholds - verbatim from config/thresholds.yaml (keep in sync).
THRESHOLDS = {
    "pressure_bar":  {"type": "numeric", "warn": [140, 290], "critical": [120, 310]},
    "temp_celsius":  {"type": "numeric", "warn": [55, 95],   "critical": [45, 105]},
    "flow_rate_m3h": {"type": "numeric", "warn": [750, 1250],"critical": [600, 1400]},
    "vibration_g":   {"type": "numeric", "warn": [-1, 3.0],  "critical": [-1, 4.5]},
    "valve_status":  {"type": "categorical", "warn": ["PARTIAL"], "critical": ["FAULT"],
                      "normal": ["OPEN", "CLOSED"]},
}

PG = {"host": "localhost", "port": 5434, "dbname": "oilgas", "user": "kermit", "password": "kermit"}
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "sensors.normalised"
CONSUMER_GROUP = "oil_gas_anomaly_recorder"


def classify(metric, value):
    """Return (severity, threshold_type). CRITICAL checked before WARN."""
    t = THRESHOLDS.get(metric)
    if t is None:
        return "WARN", "unknown_metric"
    if t["type"] == "categorical":
        v = "" if value is None else str(value)
        if v in t["critical"]:
            return "CRITICAL", "categorical"
        if v in t["warn"]:
            return "WARN", "categorical"
        if v in t.get("normal", []):
            return "NORMAL", "normal"
        return "WARN", "unexpected"
    # numeric
    if value is None:
        return "CRITICAL", "null_value"
    try:
        x = float(value)
    except (TypeError, ValueError):
        return "CRITICAL", "bad_value"
    c, w = t["critical"], t["warn"]
    if x < c[0] or x > c[1]:
        return "CRITICAL", "critical_high" if x > c[1] else "critical_low"
    if x < w[0] or x > w[1]:
        return "WARN", "warn_high" if x > w[1] else "warn_low"
    return "NORMAL", "normal"


def numeric_or_none(value):
    """anomaly_events.value is numeric; return float or None for non-numeric (valve)."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-beginning", action="store_true")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--batch", type=int, default=50)
    args = ap.parse_args()

    from kafka import KafkaConsumer
    import psycopg2

    conn = psycopg2.connect(**PG)
    conn.autocommit = False
    cur = conn.cursor()
    print(f"Postgres connected: {PG['dbname']}.anomaly_events")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=8000 if args.once else None,
    )
    print(f"Kafka consumer on '{KAFKA_TOPIC}' "
          f"({'earliest' if args.from_beginning else 'latest'})")

    INSERT = (
        "INSERT INTO anomaly_events "
        "(well_id, field_name, event_ts, metric, value, severity, threshold_type) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )

    recorded = 0
    pending = 0
    try:
        for msg in consumer:
            e = msg.value
            metric = e.get("metric")
            value = e.get("value")
            severity, ttype = classify(metric, value)
            if severity == "NORMAL":
                continue   # only WARN/CRITICAL are anomalies

            row = (
                e.get("well_id"),
                e.get("field_name"),
                e.get("timestamp"),       # ISO string; psycopg2/PG casts to timestamptz fine
                metric,
                numeric_or_none(value),
                severity,
                ttype,
            )

            if args.dry_run:
                print("WOULD INSERT:", row)
                break

            cur.execute(INSERT, row)
            pending += 1
            recorded += 1
            if pending >= args.batch:
                conn.commit()
                pending = 0
                print(f"  recorded {recorded} anomalies...")
    except KeyboardInterrupt:
        print("\nShutdown signal received.")
    finally:
        if pending and not args.dry_run:
            conn.commit()
        cur.close()
        conn.close()
        consumer.close()
        print(f"Done. Recorded {recorded} anomalies to anomaly_events.")


if __name__ == "__main__":
    main()
