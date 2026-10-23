# Oil & Gas Pipeline — Stage-by-Stage Demo Runbook

**Goal:** prove data physically moving through every system, one hop at a time.
Each stage = one claim + one command + one visible proof.

> Keep 3 terminals open: **T1** (simulator), **T2** (queries/commands), **T3** (scripts).
> Keep these browser tabs open: StreamSets canvas · Grafana (localhost:3001) ·
> Mailhog (localhost:8025) · MinIO console (localhost:9090) · Ops Console.

---

## SETUP (before the audience arrives)
- StreamSets engine `amazing_benz` running; both jobs ACTIVE (ingestion + anomaly).
- `mongo_archiver.py` running (T3).
- `ops_dashboard_api.py` running; Ops Console open in a tab.
- A few minutes of `--realistic` data already flowing so nothing looks empty.
- Have this runbook open.

---

## STAGE 0 — The wellheads (MQTT source)
**Claim:** 12 wellheads publish JSON sensor events to MQTT topics
`sensors/wellhead/{id}/{metric}`.

**T1 — start the simulator (realistic baseline):**
```
python wellhead_simulator.py --realistic
```
**Point at:** the simulator log —每 cycle shows 12 wells, per-metric readings,
NORMAL/WARN/CRITICAL tags. "These are the raw sensor events entering the system."

**(Optional) prove MQTT itself is carrying them — T2:**
```
docker exec oilgas-mosquitto mosquitto_sub -t 'sensors/wellhead/+/+' -C 5
```
Shows 5 raw JSON messages straight off the broker. "This is the wire — before
StreamSets touches anything."

---

## STAGE 1 — MQTT Ingestion Pipeline (StreamSets)
**Claim:** StreamSets subscribes to MQTT, enriches headers, tags data quality,
and routes — good → Kafka, bad → DLQ.

**Show:** the **ingestion pipeline canvas** in StreamSets. Walk the stages
left→right:
- MQTT Subscriber (topic `sensors/wellhead/+/+`)
- kafka_header_enricher (adds metadata)
- data_quality_tagger (sets quality_score: 1.0 ok / 0.0 bad)
- Stream Selector → **two lanes**: quality_score==0.0 → DLQ ; default → Kafka

**Point at:** the live record counts climbing on each stage. "Every reading is
enriched and quality-checked here."

**Take a snapshot** of the running job → open one record → show the added fields
(`quality_score`, `quality_status`, enriched headers). That's the proof the
enrichment + tagging actually happened.

---

## STAGE 2 — Kafka normalised topic
**Claim:** clean, normalised readings land on Kafka topic `sensors.normalised`.

**T2 — consume a few messages straight off Kafka:**
```
docker exec oilgas-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic sensors.normalised --max-messages 3
```
**Point at:** the 3 JSON messages with `quality_score: 1.0`. "Only clean data
reaches this topic — the normalised stream everything downstream consumes."

---

## STAGE 3 — Dead-Letter Queue (MinIO)
**Claim:** bad readings (nulls/out-of-range) are NEVER dropped — they go to MinIO
`kermit-error-dlq` with full metadata for reprocessing. (Compliance requirement.)

**T1 — stop --realistic (Ctrl+C), then inject bad data:**
```
python wellhead_simulator.py --scenario dlq-demo
```
(this nulls well 9's readings → quality_score 0.0 → quarantine lane)

**Point at:**
- StreamSets canvas: the DLQ lane count ticks up.
- **MinIO console** (localhost:9090) → bucket `kermit-error-dlq` → prefix
  `oil_gas/` → open a JSON object → show the error envelope
  (`dlq_failure_stage`, `dlq_source`, original payload).
"Zero events silently dropped — the bad ones are captured with full context."

Then **Ctrl+C** and go back to:
```
python wellhead_simulator.py --realistic
```

---

## STAGE 4 — MongoDB archive (secondary write)
**Claim:** every raw event is archived to MongoDB `raw_sensor_events` — a
non-blocking secondary write, decoupled from the pipeline.

**T2 — show the archive growing:**
```
docker exec oilgas-mongodb mongosh -u kermit -p kermit --authenticationDatabase admin \
  --quiet --eval "db=db.getSiblingDB('oilgas'); print('docs:', db.raw_sensor_events.countDocuments()); printjson(db.raw_sensor_events.findOne())"
```
**Point at:** the doc count (climbing) + one sample document. "Full history
retained in Mongo for replay/audit, separate from the real-time path."

---

## STAGE 5 — Anomaly Detection Pipeline (classify + route)
**Claim:** each reading is classified against thresholds; anomalies are written
to Postgres `anomaly_events`; CRITICALs additionally trigger a maintenance case.

**Show:** the **anomaly detection pipeline canvas**:
- Kafka consumer → JavaScript classifier (NORMAL/WARN/CRITICAL)
- Stream Selector 1: anomaly → Field Converter → JDBC (anomaly_events) ;
  normal → Trash
- Stream Selector 2: CRITICAL → alert dispatcher → maintenance API ; WARN → Trash

**T2 — show anomalies landing in Postgres:**
```
docker exec oilgas-postgres psql -U kermit -d oilgas -c \
"SELECT well_id, metric, value, severity, threshold_type, event_ts FROM anomaly_events ORDER BY id DESC LIMIT 8;"
```
"Every WARN and CRITICAL is logged here BEFORE any API call — compliance: an
anomaly is never lost even if the API is down."

---

## STAGE 6 — CRITICAL → Maintenance Case (the alert)
**Claim:** every CRITICAL automatically opens a maintenance case via the API
(with retry/backoff).

**T1 — inject a CRITICAL on well 7:**
```
python wellhead_simulator.py --force-critical 7
```
**Point at (within a few seconds):**
- **Ops Console** alert feed: WELL-07 critical flashes in; "Wells in Alarm" goes red.
- **T2 — the maintenance case created:**
```
docker exec oilgas-postgres psql -U kermit -d oilgas -c \
"SELECT case_id, well_id, anomaly_type, severity, assigned_team, recorded_at FROM maintenance_cases ORDER BY recorded_at DESC LIMIT 5;"
```
"Out-of-range pressure → CRITICAL → case opened automatically, routed to the
right team."

---

## STAGE 7 — Blowout Watchdog (stateful escalation)
**Claim:** when one well throws 3+ CRITICAL in 10 min (blowout pattern), the
watchdog escalates: case + supervisor email + pauses ingestion.

**(well 7 is already flooding criticals from Stage 6) — T3:**
```
python anomaly_watchdog.py --once --pause-mode soft
```
**Point at:**
- Terminal log: `*** BLOWOUT DETECTED *** WELL-07`.
- **Mailhog** (localhost:8025): the "BLOWOUT: WELL-07" email to the supervisor.
"This is the stateful detection a single-record stage can't do — cross-record
pattern over time, with automatic escalation."

(Then Ctrl+C the force-critical sim; back to --realistic.)

---

## STAGE 8 — Shift Report (scheduled deliverable)
**Claim:** automated Excel shift report with chart, delivered to MinIO + emailed
to the supervisor.

**T3:**
```
python shift_report_generator.py --shift morning
```
**Point at:**
- **MinIO** `kermit-shift-reports` → the `.xlsx` (open/preview → show the
  per-well table + anomaly-by-hour chart).
- **Mailhog**: the shift-report email with the xlsx attached.

---

## STAGE 9 — Grafana + Ops Console (monitoring)
**Claim:** live operational visibility.
- **Grafana** (localhost:3001): the 4 panels — anomalies/well/hour, CRITICAL 24h,
  cases today, anomaly rate. (Required slide: screenshot with a CRITICAL visible.)
- **Ops Console**: the unified operator view — feed, cases, charts, live.

---

## STAGE 10 — SDK: onboard a new wellhead (no UI)
**Claim:** a new well can be onboarded from the CLI; design = one parameterised
pipeline (justified).

**T2:**
```
python wellhead_pipeline_manager.py add-wellhead --well WELL-13 --field Field-West
python wellhead_pipeline_manager.py generate --dry-run
```
**Point at:** registry updated, "covered by wildcard — no new pipeline needed,"
pipeline rebuilds for 13 wells. "Scales to N wells with zero per-well duplication."

---

## CLOSING LINE
"End to end: raw MQTT telemetry → enriched, quality-checked, normalised to Kafka →
archived to Mongo, classified for anomalies → logged to Postgres → CRITICALs open
maintenance cases → blowout patterns escalate with automatic pause → shift reports
delivered → all monitored live. Every stage with zero data loss and full audit."

---

## ONE-LINE CHEAT SHEET (commands in order)
```
# baseline
python wellhead_simulator.py --realistic
docker exec oilgas-mosquitto mosquitto_sub -t 'sensors/wellhead/+/+' -C 5
docker exec oilgas-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sensors.normalised --max-messages 3
# dlq
python wellhead_simulator.py --scenario dlq-demo
# mongo
docker exec oilgas-mongodb mongosh -u kermit -p kermit --authenticationDatabase admin --quiet --eval "db=db.getSiblingDB('oilgas'); print(db.raw_sensor_events.countDocuments())"
# anomalies + critical
docker exec oilgas-postgres psql -U kermit -d oilgas -c "SELECT well_id,metric,value,severity FROM anomaly_events ORDER BY id DESC LIMIT 8;"
python wellhead_simulator.py --force-critical 7
docker exec oilgas-postgres psql -U kermit -d oilgas -c "SELECT case_id,well_id,severity FROM maintenance_cases ORDER BY recorded_at DESC LIMIT 5;"
# blowout
python anomaly_watchdog.py --once --pause-mode soft
# shift report
python shift_report_generator.py --shift morning
# onboard
python wellhead_pipeline_manager.py add-wellhead --well WELL-13 --field Field-West
python wellhead_pipeline_manager.py generate --dry-run
```
