#!/usr/bin/env python3
"""
anomaly_watchdog.py — KSEA Oil & Gas blowout watchdog (brief 5.3)
=================================================================
Runs continuously, polling the Postgres `anomaly_events` table every 60 seconds.
Detects the BLOWOUT INDICATOR pattern — the same well producing 3 or more
CRITICAL events within a 10-minute window — and on detection:

  1. Fires a CRITICAL alert by POSTing a case to the maintenance API.
  2. Pauses the ingestion job in StreamSets Control Hub (SDK stop_job) so the
     field's telemetry halts pending engineer review.
  3. Records the open blowout in a local JSON state file so it is NOT re-alerted
     every cycle. State persists across restarts and is written atomically.

This watchdog owns the STATEFUL detection that the alert_notification_dispatcher
fragment cannot do (a fragment sees one record at a time). The fragment handles
per-record CRITICAL dispatch; this handles the cross-record blowout pattern.

Scaffolding (atomic state writes with corrupt-file quarantine, the notifier
abstraction, the dataclass state model, SDK stop/wait job-control) is adapted
from the team's self-healing job watchdog (sdc_watchdog.py), retargeted from
job-health monitoring to anomaly-data monitoring and Teams -> maintenance API.

DESIGN NOTE (for sign-off):
  The brief says "pause the ingestion pipeline for that well." Ingestion is ONE
  parameterised pipeline shared by all 12 wells, so there is no per-well job to
  stop in isolation. Two honest options:
    (a) Stop the shared ingestion JOB — halts ALL wells. A blowout is a
        field-wide emergency, so a total stop pending review is defensible.
    (b) Soft-pause — record the well as paused in state; a downstream filter
        drops that well's records, the job keeps running for other wells.
  Default is (a) --pause-mode job; (b) via --pause-mode soft. Explain the
  tradeoff in the demo.

State file schema (local JSON):
  { "schema_version": 1,
    "open_blowouts": { "WELL-07": {"opened_at","case_id","event_count",
                                    "paused","pause_mode"} },
    "updated_at": "..." }

Usage:
  python anomaly_watchdog.py                    # run continuously (60s)
  python anomaly_watchdog.py --once             # single poll (testing)
  python anomaly_watchdog.py --dry-run          # detect + log, no API/stop
  python anomaly_watchdog.py --pause-mode soft  # don't stop the job
  python anomaly_watchdog.py --window-min 10 --threshold 3 --interval 60

Secrets (Control Hub creds) come from fragments/sch_config.yaml, same as the
build scripts. Deps: psycopg2-binary, requests, streamsets, pyyaml.
"""
import argparse
import json
import logging
import os
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
PG = {"host": "localhost", "port": 5434, "dbname": "oilgas", "user": "kermit", "password": "kermit"}
MAINTENANCE_API_URL = "http://localhost:8000/cases"   # host-side; engine uses maintenance-api:8000
STATE_FILE = str(HERE / "watchdog_state.json")
SCH_CONFIG = str(HERE.parent / "fragments" / "sch_config.yaml")
INGESTION_JOB_MATCH = "oil_gas__mqtt_ingestion"        # substring of "Job for oil_gas__mqtt_ingestion"

DEFAULT_WINDOW_MIN = 10
DEFAULT_THRESHOLD = 3
DEFAULT_INTERVAL_S = 60
WAIT_TIMEOUT_S = 180

logger = logging.getLogger("anomaly-watchdog")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# State model + persistence (atomic write + corrupt-file quarantine)
# ---------------------------------------------------------------------------
@dataclass
class Blowout:
    well_id: str
    opened_at: str
    case_id: str = None
    event_count: int = 0
    paused: bool = False
    pause_mode: str = "job"

    @classmethod
    def from_dict(cls, d):
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        logger.info("No state file at %s — starting fresh", STATE_FILE)
        return {"schema_version": 1, "open_blowouts": {}}
    try:
        with p.open() as f:
            data = json.load(f)
        if data.get("schema_version") != 1:
            raise ValueError(f"unsupported schema_version {data.get('schema_version')}")
        data.setdefault("open_blowouts", {})
        return data
    except (json.JSONDecodeError, OSError, ValueError) as e:
        quarantine = p.with_suffix(f".corrupt.{int(time.time())}")
        try:
            p.rename(quarantine)
        except OSError:
            pass
        logger.error("State file unreadable (%s); quarantined to %s. Starting fresh.", e, quarantine)
        return {"schema_version": 1, "open_blowouts": {}}


def save_state(state: dict) -> None:
    p = Path(STATE_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = _now_iso()
    tmp = p.with_suffix(".tmp")
    with tmp.open("w") as f:
        json.dump(state, f, indent=2, default=str)
    tmp.replace(p)   # atomic


# ---------------------------------------------------------------------------
# Notifier (adapted from sdc_watchdog; logs + optional Mailhog email)
# ---------------------------------------------------------------------------
def make_notifier(mailhog_enabled: bool):
    """Returns notify(subject, body). Logs always; emails via Mailhog SMTP if enabled."""
    def notify(subject: str, body: str) -> None:
        logger.info("NOTIFY: %s — %s", subject, body)
        if not mailhog_enabled:
            return
        try:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(body)
            msg["Subject"] = subject
            msg["From"] = "watchdog@oilgas.local"
            msg["To"] = "shift-supervisor@oilgas.local"
            with smtplib.SMTP("localhost", 1025, timeout=10) as s:  # Mailhog SMTP
                s.send_message(msg)
        except Exception as e:
            logger.error("Mailhog notify failed: %s", e)
    return notify


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------
def detect_blowouts(cur, window_min: int, threshold: int) -> dict:
    """Return {well_id: count} for wells with >= threshold CRITICAL in the window."""
    cur.execute(
        """
        SELECT well_id, COUNT(*) AS crit_count
        FROM anomaly_events
        WHERE severity = 'CRITICAL'
          AND event_ts >= (NOW() - (%s || ' minutes')::interval)
        GROUP BY well_id
        HAVING COUNT(*) >= %s
        ORDER BY crit_count DESC
        """,
        (window_min, threshold),
    )
    return {row[0]: int(row[1]) for row in cur.fetchall()}


# ---------------------------------------------------------------------------
# Actions: maintenance case + pause ingestion
# ---------------------------------------------------------------------------
def fire_maintenance_case(well_id: str, count: int, dry_run: bool):
    payload = {
        "well_id": well_id,
        "anomaly_type": "blowout_pattern",
        "severity": "CRITICAL",
        "timestamp": _now_iso(),
    }
    if dry_run:
        logger.info("  [dry-run] would POST case: %s", payload)
        return f"DRY-{well_id}"
    import requests
    try:
        resp = requests.post(MAINTENANCE_API_URL, json=payload, timeout=10)
        if resp.status_code in (200, 201):
            cid = (resp.json() if resp.content else {}).get("case_id", "UNKNOWN")
            logger.info("  maintenance case opened: %s", cid)
            return cid
        logger.error("  maintenance API %s: %s", resp.status_code, resp.text[:120])
    except Exception as e:
        logger.error("  maintenance API call failed: %s", e)
    return None


def pause_ingestion_job(pause_mode: str, dry_run: bool) -> bool:
    if pause_mode == "soft":
        logger.info("  soft-pause: well marked paused (job left running)")
        return True
    if dry_run:
        logger.info("  [dry-run] would STOP ingestion job matching '%s'", INGESTION_JOB_MATCH)
        return True
    try:
        import yaml
        from streamsets.sdk import ControlHub
        ch = yaml.safe_load(open(SCH_CONFIG))["control_hub"]
        sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])
        jobs = [j for j in sch.jobs if INGESTION_JOB_MATCH in getattr(j, "job_name", "")]
        if not jobs:
            logger.warning("  no ingestion job matching '%s' to stop", INGESTION_JOB_MATCH)
            return False
        job = jobs[0]
        st = getattr(job, "status", None)
        active = getattr(st, "status", None) == "ACTIVE"
        if active:
            sch.stop_job(job)
            sch.wait_for_job_status(job, status="INACTIVE", timeout_sec=WAIT_TIMEOUT_S)
            logger.info("  STOPPED ingestion job '%s' (blowout — engineer review)", job.job_name)
        else:
            logger.info("  ingestion job '%s' already not ACTIVE", job.job_name)
        return True
    except Exception as e:
        logger.error("  job stop failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------------
def poll_once(cur, state: dict, notify, args) -> None:
    now = _now_iso()
    blowouts = detect_blowouts(cur, args.window_min, args.threshold)
    open_b = state["open_blowouts"]

    if not blowouts:
        logger.info("no blowout pattern (>= %d CRITICAL / %dmin / well)",
                    args.threshold, args.window_min)

    for well_id, count in blowouts.items():
        if well_id in open_b:
            open_b[well_id]["event_count"] = count
            logger.info("blowout STILL OPEN %s (%d CRITICAL) — already alerted", well_id, count)
            continue
        # NEW blowout
        logger.warning("*** BLOWOUT DETECTED *** %s: %d CRITICAL in %dmin",
                       well_id, count, args.window_min)
        case_id = fire_maintenance_case(well_id, count, args.dry_run)
        paused = pause_ingestion_job(args.pause_mode, args.dry_run)
        bo = Blowout(well_id=well_id, opened_at=now, case_id=case_id,
                     event_count=count, paused=paused, pause_mode=args.pause_mode)
        open_b[well_id] = asdict(bo)
        notify(
            f"BLOWOUT: {well_id}",
            f"Blowout pattern detected on {well_id}: {count} CRITICAL events in "
            f"{args.window_min} minutes. Maintenance case {case_id} opened. "
            f"Ingestion {'stopped' if (paused and args.pause_mode=='job') else 'soft-paused'} "
            f"pending engineer review. ({now})",
        )

    # Clear blowouts that decayed below threshold so they can re-fire if recurring.
    for well_id in list(open_b.keys()):
        if well_id not in blowouts:
            logger.info("blowout cleared for %s (no longer over threshold)", well_id)
            del open_b[well_id]

    if not args.dry_run:
        save_state(state)


def main():
    ap = argparse.ArgumentParser(description="Blowout watchdog (brief 5.3)")
    ap.add_argument("--once", action="store_true", help="single poll then exit")
    ap.add_argument("--dry-run", action="store_true", help="detect + log, no API/stop/state")
    ap.add_argument("--pause-mode", choices=["job", "soft"], default="job")
    ap.add_argument("--window-min", type=int, default=DEFAULT_WINDOW_MIN)
    ap.add_argument("--threshold", type=int, default=DEFAULT_THRESHOLD)
    ap.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S)
    ap.add_argument("--no-email", action="store_true", help="disable Mailhog email")
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    import psycopg2
    conn = psycopg2.connect(**PG)
    conn.autocommit = True
    cur = conn.cursor()
    state = load_state()
    notify = make_notifier(mailhog_enabled=not args.no_email and not args.dry_run)

    logger.info(
        "Blowout watchdog started: >= %d CRITICAL / %dmin / well, every %ds, "
        "pause-mode=%s%s", args.threshold, args.window_min, args.interval,
        args.pause_mode, " [DRY-RUN]" if args.dry_run else "",
    )
    logger.info("State file: %s", STATE_FILE)

    try:
        if args.once:
            poll_once(cur, state, notify, args)
        else:
            while True:
                poll_once(cur, state, notify, args)
                time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Watchdog stopped.")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
