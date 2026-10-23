#!/usr/bin/env python3
"""
build_audit_log_writer.py  (GENERIC / Pattern A) - v2.0.0
=========================================================
Shared audit fragment for ALL tracks. One run does everything:
  1) ensures the audit table has the columns this fragment writes
     (adds audit_metrics jsonb if missing) - no separate ALTER step
  2) builds + publishes the fragment

PATTERN A - universal core + per-track metrics:
  Writes the UNIVERSAL fields every track shares:
      pipeline_name, run_ts, operator, environment
  plus a flexible /audit_metrics JSON column for per-track metrics
  (oil&gas: record_count/error_count; manufacturing: defect_count/
  critical_count/quarantine_registrations/...). Each track sets its own
  metrics via the AUDIT_METRICS_JSON param - the fragment never changes
  when a new track appears.

CONNECTION fully parameterized (any JDBC DB incl. Postgres):
  AUDIT_JDBC_URL, AUDIT_SCHEMA, AUDIT_TABLE, AUDIT_DB_USER, AUDIT_DB_PASSWORD

Requires: pip install psycopg2-binary   (for the auto column-ensure step)
If psycopg2 isn't installed, the script still builds/publishes and just
prints a reminder to add the column manually.

Usage:
    python build_audit_log_writer.py --dry-run
    python build_audit_log_writer.py
    python build_audit_log_writer.py --skip-db   # don't touch the DB
"""
import argparse
import yaml
from streamsets.sdk import ControlHub

FRAGMENT_NAME = "audit_log_writer"
FRAGMENT_VERSION = "2.0.0"

DEFAULT_PARAMETERS = {
    "AUDIT_JDBC_URL": "jdbc:postgresql://postgres:5432/oilgas",
    "AUDIT_SCHEMA": "public",
    "AUDIT_TABLE": "audit_log",
    "AUDIT_DB_USER": "kermit",
    "AUDIT_DB_PASSWORD": "kermit",
    "AUDIT_OPERATOR": "ksea_oil_gas",
    "AUDIT_ENVIRONMENT": "dev",
    "AUDIT_METRICS_JSON": "{}",
}

# Host-side connection for the column-ensure step (psql via host port 5434).
# This is SEPARATE from the JDBC param the engine uses (postgres:5432 internal).
DB_HOST_CONN = {
    "host": "localhost",
    "port": 5434,            # host->container mapping for oilgas-postgres
    "dbname": "oilgas",
    "user": "kermit",
    "password": "kermit",
}
AUDIT_TABLE_LOCAL = "audit_log"


def load_config(path="sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def ensure_audit_column():
    """Add audit_metrics jsonb to the audit table if missing. Idempotent."""
    try:
        import psycopg2
    except ImportError:
        print("  (psycopg2 not installed - skipping auto column add.)")
        print("  Install with: pip install psycopg2-binary  OR add the column manually:")
        print("    ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS audit_metrics JSONB;")
        return
    try:
        conn = psycopg2.connect(**DB_HOST_CONN)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {AUDIT_TABLE_LOCAL} "
                f"ADD COLUMN IF NOT EXISTS audit_metrics JSONB;"
            )
        conn.close()
        print(f"  DB: ensured '{AUDIT_TABLE_LOCAL}.audit_metrics' column exists.")
    except Exception as e:
        print(f"  (could not reach DB to add column: {e})")
        print("  Add manually if needed:")
        print("    ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS audit_metrics JSONB;")


def main():
    ap = argparse.ArgumentParser(description="Build & publish audit_log_writer (Pattern A)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-db", action="store_true", help="don't touch the database")
    ap.add_argument("--config", default="sch_config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ch, eng, pub = cfg["control_hub"], cfg["engine"], cfg["publish"]

    # 1) ensure DB column (unless skipped or dry-run)
    if not args.skip_db and not args.dry_run:
        print("Ensuring audit table column ...")
        ensure_audit_column()

    print(f"Connecting to Control Hub: {ch['url']}")
    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

    print(f"Building fragment '{FRAGMENT_NAME}' v{FRAGMENT_VERSION} (Pattern A) ...")
    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"], fragment=True,
    )

    expr = builder.add_stage("Expression Evaluator")
    expr.field_expressions = [
        {"fieldToSet": "/pipeline_name", "expression": "${pipeline:name()}"},
        {"fieldToSet": "/run_ts",        "expression": "${time:now()}"},
        {"fieldToSet": "/operator",      "expression": "${AUDIT_OPERATOR}"},
        {"fieldToSet": "/environment",   "expression": "${AUDIT_ENVIRONMENT}"},
        {"fieldToSet": "/audit_metrics", "expression": "${AUDIT_METRICS_JSON}"},
    ]

    jdbc = builder.add_stage("JDBC Producer", type="destination")
    jdbc.jdbc_connection_string = "${AUDIT_JDBC_URL}"
    jdbc.jdbc_driver_class_name = "org.postgresql.Driver"
    jdbc.schema_name = "${AUDIT_SCHEMA}"
    jdbc.table_name = "${AUDIT_TABLE}"
    jdbc.use_credentials = True
    jdbc.username = "${AUDIT_DB_USER}"
    jdbc.password = "${AUDIT_DB_PASSWORD}"
    jdbc.field_to_column_mapping = [
        {"columnName": "pipeline_name", "field": "/pipeline_name", "paramValue": "?"},
        {"columnName": "run_ts",        "field": "/run_ts",        "paramValue": "?"},
        {"columnName": "operator",      "field": "/operator",      "paramValue": "?"},
        {"columnName": "environment",   "field": "/environment",   "paramValue": "?"},
        {"columnName": "audit_metrics", "field": "/audit_metrics", "paramValue": "?"},
    ]

    expr >> jdbc

    fragment = builder.build(
        title=FRAGMENT_NAME,
        description=(
            "Shared audit writer (Pattern A). Writes universal run-audit fields "
            "(pipeline_name, run_ts, operator, environment) to any JDBC audit "
            "table. Per-track metrics ride in the parameterized /audit_metrics "
            "JSON column. Connection + values are runtime params. KSEA shared fragment."
        ),
        labels=pub["labels"],
    )
    fragment.parameters = DEFAULT_PARAMETERS

    if args.dry_run:
        print("DRY RUN - fragment built but NOT published. (DB not touched.)")
        print("  Universal fields: pipeline_name, run_ts, operator, environment, audit_metrics")
        for k, v in DEFAULT_PARAMETERS.items():
            print(f"    {k} = {v}")
        return

    print(f"Publishing to Control Hub (workspace: {pub['workspace']}) ...")
    sch.publish_pipeline(
        fragment,
        commit_message=f"{FRAGMENT_NAME} v{FRAGMENT_VERSION} - Pattern A universal core + metrics (KSEA)",
    )
    print(f"Published '{FRAGMENT_NAME}' v{FRAGMENT_VERSION}.")


if __name__ == "__main__":
    main()
