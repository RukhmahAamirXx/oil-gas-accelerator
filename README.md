# Real-Time Wellhead Anomaly Detection

**KSEA StreamSets Engineering Accelerator — Oil & Gas Track**
Rukhmah Aamir · KermitTech · Built on StreamSets SDK 6.6.2

A real-time anomaly detection system for 12 offshore wellheads. Sensor readings flow over MQTT, get enriched and quality-checked, are routed clean → Kafka / bad → MinIO dead-letter queue, archived to MongoDB, classified against configurable thresholds, logged to Postgres, and — when CRITICAL — automatically opened as maintenance cases. A stateful watchdog escalates blowout patterns via email; a scheduled script produces shift reports.

---

## Architecture

Diagram source: [`docs/oilgas_architecture.drawio`](docs/oilgas_architecture.drawio) — open it in [diagrams.net](https://app.diagrams.net) (File → Open from → Device) or the VS Code Draw.io extension.

```
Wellhead sensors → MQTT (Mosquitto)
  → Ingestion pipeline: Header Enricher → Data Quality Tagger → quality_score?
       ├─ bad (0.0)   → Dead-Letter Queue (MinIO kermit-error-dlq)
       └─ clean (1.0) → Kafka sensors.normalised
                            ├─ archive → MongoDB raw_sensor_events
                            └─ Anomaly Detection pipeline: classifier (thresholds.yaml) → severity?
                                 ├─ NORMAL            → discarded
                                 ├─ WARN + CRITICAL   → Postgres anomaly_events
                                 └─ CRITICAL only     → Maintenance API (opens case)
Postgres → Blowout Watchdog (3+ CRITICAL / 10 min / well) → Mailhog email
Postgres → Shift Report Generator → Excel → MinIO kermit-shift-reports + email
Postgres → Grafana, Ops Console
```

---

## What's in the box

```
oil-gas-acceleratorX/
├── wellhead_simulator.py            # publishes synthetic MQTT data
├── config/
│   ├── thresholds.yaml              # per-metric WARN/CRITICAL bands
│   └── wellheads.yaml               # well registry (used by pipeline_manager)
├── fragments/
│   ├── build_kafka_header_enricher.py         # generic, parameterised
│   ├── build_data_quality_tagger.py           # generic
│   ├── build_alert_notification_dispatcher.py # generic, parameterised
│   ├── build_audit_log_writer.py
│   ├── build_dead_letter_queue_handler.py
│   └── sch_config.yaml.template     # copy to sch_config.yaml (gitignored) and fill in
├── pipelines/
│   ├── build_mqtt_ingestion_pipeline.py       # builds + publishes ingestion
│   ├── build_anomaly_detection_pipeline.py    # builds + publishes detection
│   └── wellhead_pipeline_manager.py           # SDK 5.1 — onboard wells
├── scripts/
│   ├── anomaly_watchdog.py          # SDK 5.3 — blowout detection
│   ├── shift_report_generator.py    # SDK 5.2 — Excel + email
│   ├── mongo_archiver.py            # Kafka → MongoDB archive
│   ├── audit_run_logger.py          # writes audit_log row per run
│   └── reset_data.py                # clean reset (Postgres + Mongo + MinIO)
├── ops_console/
│   ├── ops_dashboard_api.py         # read-only Flask API on :8050
│   └── ops_console.html             # unified operator UI
├── grafana/
│   └── provisioning/                # datasource + dashboard provisioning
├── docs/oilgas_architecture.drawio  # architecture diagram (diagrams.net)
├── DEMO_RUNBOOK.md                  # 11-stage demo script
├── DEMO_COMMANDS_ONLY.md            # demo commands, no commentary
├── start_demo.sh / stop_demo.sh     # one-command demo start/stop
├── docker-compose.yml               # full local stack
└── requirements.txt
```

---

## Prerequisites

- **Docker** (with the oil & gas stack containers running)
- **Python 3.9+** with a virtualenv
- **StreamSets engine** running and registered with Control Hub
- Control Hub credentials: `cp fragments/sch_config.yaml.template fragments/sch_config.yaml`, then fill in your credential id/token and engine id. This file is gitignored — never commit it.

The Docker stack provides: MQTT broker (Mosquitto), Kafka, Postgres, MongoDB, MinIO, Grafana, Mailhog, maintenance API.

---

## Setup

```bash
# 1. Clone / unzip the project
cd oil-gas-acceleratorX

# 2. Create + activate a virtualenv
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

That installs the StreamSets SDK, Kafka, Mongo, Postgres, MinIO, and HTTP clients needed by the scripts.

---

## Running the system

A typical run uses three terminals:

**Terminal 1 — simulator (continuous data source):**
```bash
python wellhead_simulator.py            # default: light random anomalies
# Other modes:
# python wellhead_simulator.py --clean             # all NORMAL
# python wellhead_simulator.py --realistic         # heavier stress test
# python wellhead_simulator.py --force-critical 7  # flood well 7 with CRITICAL
# python wellhead_simulator.py --scenario dlq-demo # null readings → DLQ
```

**Terminal 2 — MongoDB archiver (Kafka → Mongo):**
```bash
python scripts/mongo_archiver.py
```

**Terminal 3 — blowout watchdog (continuous):**
```bash
python scripts/anomaly_watchdog.py --pause-mode soft
```

The StreamSets ingestion and anomaly detection pipelines run on the engine and are managed via the Control Hub job UI.

---

## How to trigger a CRITICAL scenario

The brief requires this. The simulator can deliberately produce CRITICAL anomalies on any well:

```bash
# Floods WELL-07 with CRITICAL readings (pressure > 310 bar, temp > 105°C, etc.)
python wellhead_simulator.py --force-critical 7
```

Let it run ~30 seconds and Ctrl+C. What happens automatically:

1. **MQTT** — readings publish to `sensors/wellhead/WELL-07/*` with out-of-range values
2. **Ingestion pipeline** — readings get enriched, pass quality check (score 1.0), flow to Kafka topic `sensors.normalised`
3. **Anomaly detection pipeline** — the JavaScript classifier compares each reading to `thresholds.yaml`, sets `severity = CRITICAL`
4. **Postgres** — every CRITICAL row is logged to `anomaly_events` via JDBC
5. **Alert dispatcher** — for CRITICAL only, the dispatcher POSTs a payload to the maintenance API → a `maintenance_cases` row is created
6. **Ops console / Grafana** — counters update live; the "Wells in Alarm" indicator goes red

---

## How to verify a case was created

Three ways, from quickest to most thorough.

**1. Query Postgres directly:**
```bash
docker exec oilgas-postgres psql -U kermit -d oilgas -c \
"SELECT case_id, well_id, anomaly_type, severity, recorded_at
 FROM maintenance_cases ORDER BY recorded_at DESC LIMIT 5;"
```
You should see new `CASE-...` rows with `well_id = WELL-07` and `severity = CRITICAL`.

**2. Query the maintenance API directly:**
```bash
# List cases via the API
curl -s http://localhost:8000/cases | python3 -m json.tool | head -40

# Or check the FastAPI Swagger UI in a browser
open http://localhost:8000/docs
```

**3. Visual confirmation — Ops Console:**
Open `ops_console/ops_console.html` (with `ops_dashboard_api.py` running on :8050). The "Maintenance Cases" panel shows new cases live; the "Live Critical Alert Feed" flashes new WELL-07 alerts in.

---

## How to run a shift report

The shift report generator queries Postgres for a defined window, builds an Excel report with per-well anomaly counts and an embedded chart, uploads it to MinIO, and emails it to the supervisor via Mailhog.

```bash
# Morning shift (06:00-14:00)
python scripts/shift_report_generator.py --shift morning

# Afternoon shift (14:00-22:00)
python scripts/shift_report_generator.py --shift afternoon

# Night shift (22:00-06:00, crosses midnight)
python scripts/shift_report_generator.py --shift night

# Specific date
python scripts/shift_report_generator.py --shift morning --date 2026-06-26

# Useful flags:
#   --no-upload    skip MinIO upload
#   --no-email     skip the email
#   --local-only   just write the file locally
```

**Verify the output landed:**
- **Excel file** — open MinIO console at http://localhost:9090 → bucket `kermit-shift-reports` → the `.xlsx` file is there. Or via CLI:
  ```bash
  python3 -c "
  import boto3
  from botocore.config import Config
  s3 = boto3.client('s3', endpoint_url='https://localhost:9000',
      aws_access_key_id='kermit', aws_secret_access_key='kermit123',
      region_name='us-east-1', verify=False, config=Config(s3={'addressing_style':'path'}))
  for o in s3.list_objects_v2(Bucket='kermit-shift-reports').get('Contents', []):
      print(o['Key'])"
  ```
- **Email** — Mailhog web UI at http://localhost:8025. The shift-report email arrives with the xlsx attached.

---

## Blowout watchdog

The watchdog detects sustained failures — defined as 3 or more CRITICAL events on a single well within a 10-minute window. When detected, it: opens a `maintenance_cases` row, emails the shift supervisor via Mailhog, and pauses ingestion (soft pause by default).

```bash
# One-shot (polls once and exits)
python scripts/anomaly_watchdog.py --once --pause-mode soft

# Continuous (polls every 60 seconds, runs forever)
python scripts/anomaly_watchdog.py --pause-mode soft

# Useful flags:
#   --window-min 10      detection window in minutes
#   --threshold 3        N+ criticals to trigger
#   --pause-mode job     hard-stop the StreamSets job via SDK
#   --pause-mode soft    mark as paused but leave job running (default)
#   --dry-run            log what would happen, don't send/open anything
#   --no-email           skip the Mailhog email
```

The watchdog persists state in `scripts/watchdog_state.json` so an ongoing blowout doesn't get re-alerted. Delete the file to reset state.

**To trigger a watchdog demo:**
```bash
# 1. Create the blowout pattern
python wellhead_simulator.py --force-critical 7    # ~30s, then Ctrl+C

# 2. Run the watchdog
python scripts/anomaly_watchdog.py --once --pause-mode soft

# 3. Verify the email at http://localhost:8025
```

---

## Adding a new wellhead via SDK

The pipeline manager demonstrates the SDK 5.1 deliverable — onboarding wells from the command line without touching the UI.

```bash
# View current registry
python pipelines/wellhead_pipeline_manager.py list

# Onboard a new well
python pipelines/wellhead_pipeline_manager.py add-wellhead --well WELL-14 --field Field-West

# Rebuild the pipeline covering all wells (dry-run: build but don't publish)
python pipelines/wellhead_pipeline_manager.py generate --dry-run

# Or publish the rebuilt pipeline
python pipelines/wellhead_pipeline_manager.py generate
```

The design uses one parameterised pipeline with the MQTT wildcard `sensors/wellhead/+/+`, so a new well is automatically covered — no per-well pipeline duplication.

---

## Configurable thresholds

Edit `config/thresholds.yaml` to tune what counts as WARN or CRITICAL. The JavaScript classifier in the anomaly detection pipeline reads these at runtime.

| Metric         | Normal       | WARN          | CRITICAL          |
|----------------|--------------|---------------|-------------------|
| pressure_bar   | 150 – 280    | 140 – 290     | < 120 or > 310    |
| temp_celsius   | 60 – 90      | 55 – 95       | < 45 or > 105     |
| flow_rate_m3h  | 800 – 1200   | 750 – 1250    | < 600 or > 1400   |
| vibration_g    | 0 – 2.5      | 2.5 – 3.0     | > 4.5             |
| valve_status   | OPEN/CLOSED  | PARTIAL       | FAULT             |

Blowout pattern: 3 or more CRITICAL events on one well within 10 minutes.

---

## Monitoring

**Grafana** — http://localhost:3001
Four operational panels (anomalies per well per hour, CRITICAL count 24h, cases opened today, anomaly event rate).

**Ops Console** — `ops_console/ops_console.html` (with the API running):
```bash
python ops_console/ops_dashboard_api.py    # serves on http://localhost:8050
open ops_console/ops_console.html
```
Live unified view: KPIs, alert feed, cases, field health, anomaly rate, severity split, plus live Mailhog and MinIO panels.

**Mailhog** — http://localhost:8025
Catches all alert + shift report emails.

**MinIO** — http://localhost:9090 (credentials: `kermit / kermit123`)
- DLQ bucket: `kermit-error-dlq` (objects under `oil_gas/` prefix)
- Shift reports: `kermit-shift-reports`

---

## Resetting data

The reset script truncates the Postgres data tables, drops the Mongo archive, and optionally clears MinIO objects. It does **not** touch schemas, pipelines, fragments, or jobs — only the accumulated data.

```bash
# Preview what would be cleared
python scripts/reset_data.py --dry-run --minio

# Real reset (stops jobs first, clears Postgres + Mongo + MinIO)
python scripts/reset_data.py --stop-jobs --minio
```

Before running: stop the simulator and any consumer scripts (`mongo_archiver.py`, `anomaly_watchdog.py`). After running: restart the StreamSets jobs and the consumers, then start the simulator again.

---

## Shared fragments

Three fragments are designed to be reusable across all team members **without modification** — they use runtime parameters instead of hardcoded field names:

- **`kafka_header_enricher` v2.0.0** — params: `ID_FIELD`, `GROUP_FIELD`, `PIPELINE_VERSION_STAMP`
- **`data_quality_tagger` v3.0.0** — sets `quality_score`, `quality_status`, `quality_checked_at`
- **`alert_notification_dispatcher` v3.0.0** — params: `ID_FIELD`, `ID_OUT_KEY`, `METRIC_FIELD`, `MAINTENANCE_API_URL`

Each team member sets parameter values on their pipeline (Oil & Gas: `ID_FIELD=well_id`; Manufacturing: `ID_FIELD=machine_id`; Banking: `ID_FIELD=account_id`).

---

## Project status

All functional deliverables are verified working: MQTT ingestion, DLQ to MinIO, Mongo archive, anomaly detection to Postgres, CRITICAL → maintenance cases, blowout watchdog → email, shift report → Excel + email, pipeline manager → add-wellhead, Grafana dashboard, and the bonus ops console.

For the demo script, see `DEMO_RUNBOOK.md` — 11 stages, each with one claim, one command, and one visible proof.
