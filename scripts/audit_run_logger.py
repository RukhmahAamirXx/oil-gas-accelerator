#!/usr/bin/env python3
"""
audit_run_logger.py  - KSEA Oil & Gas run-summary audit logger
===============================================================
Writes ONE audit summary row per pipeline run to Postgres audit_log, using
job metrics pulled via the StreamSets SDK. This is the run-summary approach to
audit logging (correct for streaming jobs - one row per run, not one per
record).

Reuses the audit_log schema from the audit_log_writer fragment:
    pipeline_name, run_ts, operator, environment, audit_metrics (JSONB)
Oil & Gas metrics captured in audit_metrics:
    input_count, output_count, error_count, batch_count,
    stage_errors_count, run_count, pipeline_version

Run it after a test, on a schedule (cron), or at demo time:
    python audit_run_logger.py
    python audit_run_logger.py --job "Job for oil_gas__mqtt_ingestion"

SDK metric fields confirmed via introspection:
    input_count, output_count, error_count, batch_count,
    stage_errors_count, total_error_count, run_count, pipeline_version
"""
import argparse
import json
import datetime as dt
import yaml
import psycopg2
from streamsets.sdk import ControlHub

# --- Audit target (matches audit_log_writer fragment + combined build) -------
AUDIT_DB = {
    "host": "localhost",
    "port": 5434,          # host->container 5432
    "dbname": "oilgas",
    "user": "kermit",
    "password": "kermit",
}
AUDIT_TABLE = "audit_log"
OPERATOR = "rukhmah.aamir"
ENVIRONMENT = "dev"

DEFAULT_JOB_NAME = "Job for oil_gas__mqtt_ingestion"


def load_config(path="../fragments/sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def ensure_table(cur):
    """Create audit_log if missing (universal core + JSONB metrics)."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
            id            SERIAL PRIMARY KEY,
            pipeline_name TEXT        NOT NULL,
            run_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
            operator      TEXT,
            environment   TEXT,
            audit_metrics JSONB
        );
    """)


def main():
    ap = argparse.ArgumentParser(description="Write a run-summary audit row from job metrics.")
    ap.add_argument("--job", default=DEFAULT_JOB_NAME, help="Job name to audit")
    ap.add_argument("--config", default="../fragments/sch_config.yaml")
    ap.add_argument("--dry-run", action="store_true", help="Print the row, don't write to DB")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ch = cfg["control_hub"]
    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

    # Find the job
    matches = [j for j in sch.jobs if j.job_name == args.job]
    if not matches:
        # fall back to fuzzy match
        matches = [j for j in sch.jobs if "oil_gas" in j.job_name.lower() or "mqtt" in j.job_name.lower()]
    if not matches:
        print(f"No job found matching '{args.job}'.")
        return
    job = matches[0]
    print(f"Auditing job: {job.job_name}")

    metrics = job.metrics
    if not len(metrics):
        print("No metrics available (has the job run?). Nothing to log.")
        return
    m = metrics[0]

    # Pull the confirmed metric fields
    audit_metrics = {
        "input_count":        getattr(m, "input_count", None),
        "output_count":       getattr(m, "output_count", None),
        "error_count":        getattr(m, "error_count", None),
        "batch_count":        getattr(m, "batch_count", None),
        "stage_errors_count": getattr(m, "stage_errors_count", None),
        "total_error_count":  getattr(m, "total_error_count", None),
        "run_count":          getattr(m, "run_count", None),
        "pipeline_version":   getattr(m, "pipeline_version", None),
        # derived: records routed to DLQ are inputs that didn't reach Kafka.
        # Here output_count == records leaving the selector's default lane is
        # not separately exposed per-lane, so we record the headline counts and
        # note errors. (Per-lane DLQ counts would need stage-level metrics.)
    }
    status = str(getattr(job, "current_status", ""))

    row = {
        "pipeline_name": "oil_gas__mqtt_ingestion",
        "operator": OPERATOR,
        "environment": ENVIRONMENT,
        "audit_metrics": audit_metrics,
        "status": status,
    }

    print("Audit row to write:")
    print(f"  pipeline_name: {row['pipeline_name']}")
    print(f"  operator:      {row['operator']}")
    print(f"  environment:   {row['environment']}")
    print(f"  status:        {row['status']}")
    print(f"  audit_metrics: {json.dumps(audit_metrics, indent=2)}")

    if args.dry_run:
        print("\nDRY RUN - not written to DB.")
        return

    conn = psycopg2.connect(**AUDIT_DB)
    try:
        with conn, conn.cursor() as cur:
            ensure_table(cur)
            cur.execute(
                f"""INSERT INTO {AUDIT_TABLE}
                    (pipeline_name, run_ts, operator, environment, audit_metrics)
                    VALUES (%s, %s, %s, %s, %s)""",
                (row["pipeline_name"], dt.datetime.now(dt.timezone.utc),
                 row["operator"], row["environment"], json.dumps(audit_metrics)),
            )
        print(f"\nWrote audit row to {AUDIT_DB['dbname']}.{AUDIT_TABLE}.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
