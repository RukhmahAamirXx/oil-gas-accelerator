#!/usr/bin/env python3
"""
reset_data.py — clean all generated data for a fresh demo run.
==============================================================
Truncates the Postgres data tables, drops the MongoDB archive collection,
and (optionally) clears the MinIO DLQ + shift-report objects. Does NOT touch
schemas, pipelines, fragments, jobs, or the maintenance seed logic — only the
accumulated DATA.

IMPORTANT — stop writers FIRST (this script reminds you, doesn't force it):
  1. Stop the simulator (Ctrl+C in its terminal).
  2. Stop the StreamSets jobs (anomaly_detection + mqtt_ingestion) in Control Hub,
     OR pass --stop-jobs to have this script stop them via SDK.
  3. Stop any consumer scripts (mongo_archiver.py, anomaly_recorder.py,
     audit_run_logger loops, anomaly_watchdog.py).
Then run this.

What it clears:
  Postgres : anomaly_events, audit_log, maintenance_cases  (TRUNCATE, resets ids)
  MongoDB  : oilgas.raw_sensor_events  (drop collection)
  MinIO    : kermit-error-dlq/*, kermit-shift-reports/*  (only with --minio)

Usage:
  python reset_data.py --dry-run          # show what WOULD be cleared
  python reset_data.py                     # clear Postgres + Mongo
  python reset_data.py --minio             # also clear MinIO objects
  python reset_data.py --stop-jobs         # stop SS jobs first via SDK
  python reset_data.py --keep-cases        # keep maintenance_cases (e.g. seed row)
"""
import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PG = {"host": "localhost", "port": 5434, "dbname": "oilgas", "user": "kermit", "password": "kermit"}
MONGO_URI = "mongodb://kermit:kermit@localhost:27017/?authSource=admin"
SCH_CONFIG = str(HERE.parent / "fragments" / "sch_config.yaml")

PG_TABLES = ["anomaly_events", "audit_log", "maintenance_cases"]
MONGO_DB = "oilgas"
MONGO_COLL = "raw_sensor_events"
MINIO_BUCKETS = ["kermit-error-dlq", "kermit-shift-reports"]


def confirm(prompt):
    return input(prompt + " [y/N] ").strip().lower() == "y"


def stop_jobs():
    import yaml
    from streamsets.sdk import ControlHub
    ch = yaml.safe_load(open(SCH_CONFIG))["control_hub"]
    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])
    targets = ["oil_gas__anomaly_detection", "oil_gas__mqtt_ingestion"]
    for j in sch.jobs:
        if any(t in getattr(j, "job_name", "") for t in targets):
            st = getattr(j, "status", None)
            if getattr(st, "status", None) == "ACTIVE":
                print(f"  stopping job: {j.job_name}")
                sch.stop_job(j)
            else:
                print(f"  job already inactive: {j.job_name}")


def counts_pg(cur):
    out = {}
    for t in PG_TABLES:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        out[t] = cur.fetchone()[0]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--minio", action="store_true", help="also clear MinIO objects")
    ap.add_argument("--stop-jobs", action="store_true", help="stop SS jobs via SDK first")
    ap.add_argument("--keep-cases", action="store_true", help="do not clear maintenance_cases")
    ap.add_argument("--yes", action="store_true", help="skip confirmation")
    args = ap.parse_args()

    tables = [t for t in PG_TABLES if not (args.keep_cases and t == "maintenance_cases")]

    if args.stop_jobs and not args.dry_run:
        print("Stopping StreamSets jobs ...")
        try:
            stop_jobs()
        except Exception as e:
            print(f"  WARN: could not stop jobs via SDK: {e}")
            print("  Stop them manually in Control Hub, then re-run.")

    # --- Postgres ---
    import psycopg2
    conn = psycopg2.connect(**PG); conn.autocommit = False
    cur = conn.cursor()
    before = counts_pg(cur)
    print("\nPostgres row counts now:")
    for t, n in before.items():
        print(f"  {t:18} {n:>10,}")

    # --- Mongo ---
    from pymongo import MongoClient
    mc = MongoClient(MONGO_URI)
    mongo_n = mc[MONGO_DB][MONGO_COLL].estimated_document_count()
    print(f"\nMongoDB {MONGO_DB}.{MONGO_COLL}: {mongo_n:,} docs")

    if args.dry_run:
        print("\nDRY RUN — nothing cleared.")
        print(f"Would TRUNCATE: {', '.join(tables)}")
        print(f"Would DROP collection: {MONGO_DB}.{MONGO_COLL}")
        if args.minio:
            print(f"Would clear MinIO buckets: {', '.join(MINIO_BUCKETS)}")
        return

    if not args.yes:
        print("\nThis permanently deletes the data above (schemas/pipelines untouched).")
        if not confirm("Proceed?"):
            print("Aborted."); return

    # Postgres truncate (RESTART IDENTITY resets the id sequences)
    cur.execute(f"TRUNCATE {', '.join(tables)} RESTART IDENTITY")
    conn.commit()
    print("  Postgres tables truncated.")
    cur.close(); conn.close()

    # Mongo drop
    mc[MONGO_DB][MONGO_COLL].drop()
    print(f"  MongoDB collection {MONGO_COLL} dropped.")
    mc.close()

    # MinIO (optional)
    if args.minio:
        try:
            import boto3
            from botocore.config import Config
            s3 = boto3.client("s3", endpoint_url="https://localhost:9000",
                              aws_access_key_id="kermit", aws_secret_access_key="kermit123",
                              region_name="us-east-1", verify=False,
                              config=Config(s3={"addressing_style": "path"}))
            for b in MINIO_BUCKETS:
                try:
                    objs = s3.list_objects_v2(Bucket=b).get("Contents", [])
                    for o in objs:
                        s3.delete_object(Bucket=b, Key=o["Key"])
                    print(f"  MinIO bucket '{b}': {len(objs)} objects deleted.")
                except Exception as e:
                    print(f"  MinIO bucket '{b}': skip ({e})")
        except Exception as e:
            print(f"  MinIO clear failed: {e}")

    print("\nReset complete. Restart jobs + simulator to generate fresh data.")


if __name__ == "__main__":
    main()
