# =====================================================================
# demo_helpers.sh — named commands for the demo recording.
#
# Load in your recording terminal:
#   source ./demo_helpers.sh
#
# Then during recording, type the concise command names instead of long
# raw commands. Each function is one stage of the runbook.
# =====================================================================

PROJECT="$HOME/Documents/oil-gas-acceleratorX"

# ---- STAGE 0/1: prove MQTT is carrying data ----
show_mqtt() {
  echo "→ raw MQTT messages from Mosquitto:"
  docker exec oilgas-mosquitto mosquitto_sub -t 'sensors/wellhead/+/+' -C 5
}

# ---- STAGE 2: consume from Kafka ----
show_kafka() {
  echo "→ 3 messages from Kafka topic 'sensors.normalised':"
  docker exec oilgas-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic sensors.normalised --max-messages 3 --timeout-ms 10000 2>/dev/null
}

# ---- STAGE 3: DLQ demo (bad data → MinIO) ----
dlq_demo() {
  echo "→ injecting bad readings (well 9 nulls) for 30s..."
  cd "$PROJECT" || return 1
  timeout 30 python wellhead_simulator.py --scenario dlq-demo
  echo "→ Check MinIO console: http://localhost:9090 → kermit-error-dlq → oil_gas/"
}

# ---- STAGE 4: Mongo archive ----
show_mongo() {
  echo "→ MongoDB archive size and one sample doc:"
  docker exec oilgas-mongodb mongosh -u kermit -p kermit --authenticationDatabase admin \
    --quiet --eval "db=db.getSiblingDB('oilgas'); print('archive docs:', db.raw_sensor_events.countDocuments()); printjson(db.raw_sensor_events.findOne())"
}

# ---- STAGE 5: anomalies landing in Postgres ----
show_anomalies() {
  echo "→ latest anomalies in Postgres:"
  docker exec oilgas-postgres psql -U kermit -d oilgas -c \
    "SELECT well_id, metric, value, severity, event_ts FROM anomaly_events ORDER BY id DESC LIMIT 8;"
}

# ---- STAGE 6: inject a CRITICAL on well 7 ----
force_critical_7() {
  echo "→ flooding WELL-07 with CRITICAL readings for 30s..."
  cd "$PROJECT" || return 1
  timeout 30 python wellhead_simulator.py --force-critical 7
}

# ---- STAGE 6: verify the case was created ----
show_cases() {
  echo "→ latest maintenance cases:"
  docker exec oilgas-postgres psql -U kermit -d oilgas -c \
    "SELECT case_id, well_id, anomaly_type, severity, recorded_at FROM maintenance_cases ORDER BY recorded_at DESC LIMIT 5;"
}

# ---- STAGE 7: watchdog is CONTINUOUS in background; just show its log ----
show_watchdog_log() {
  local LOG="$HOME/oilgas-demo-logs/anomaly_watchdog.log"
  if [ -f "$LOG" ]; then
    echo "→ recent watchdog activity (blowout detections):"
    tail -30 "$LOG" | grep -E "BLOWOUT|NOTIFY|maintenance case" | tail -10
  else
    echo "→ watchdog log not found. Run manually:"
    echo "   python $PROJECT/scripts/anomaly_watchdog.py --once --pause-mode soft"
  fi
}

# ---- STAGE 8: shift report ----
shift_report() {
  local SHIFT="${1:-morning}"
  echo "→ generating $SHIFT shift report..."
  cd "$PROJECT" || return 1
  python scripts/shift_report_generator.py --shift "$SHIFT"
  echo "→ Check MinIO: http://localhost:9090 → kermit-shift-reports"
  echo "→ Check Mailhog: http://localhost:8025"
}

# ---- STAGE 10: SDK — onboard a new well ----
add_well() {
  local WELL="${1:-WELL-14}"
  local FIELD="${2:-Field-West}"
  cd "$PROJECT" || return 1
  echo "→ registry BEFORE:"
  python pipelines/wellhead_pipeline_manager.py list | tail -20
  echo
  echo "→ onboarding $WELL ($FIELD):"
  python pipelines/wellhead_pipeline_manager.py add-wellhead --well "$WELL" --field "$FIELD"
  echo
  echo "→ regenerating pipeline (dry-run):"
  python pipelines/wellhead_pipeline_manager.py generate --dry-run
}

# ---- helpers ----
demo_help() {
  cat <<'EOF'

  Demo commands (runbook stage order):

    show_mqtt              # Stage 0/1 — raw MQTT messages
    show_kafka             # Stage 2  — messages from Kafka
    dlq_demo               # Stage 3  — inject bad data → DLQ
    show_mongo             # Stage 4  — Mongo archive size
    show_anomalies         # Stage 5  — latest anomalies in Postgres
    force_critical_7       # Stage 6  — flood well 7 with CRITICAL
    show_cases             # Stage 6  — latest maintenance cases
    show_watchdog_log      # Stage 7  — blowout detections
    shift_report           # Stage 8  — generate morning shift report
    shift_report afternoon # Stage 8  — different shift
    add_well               # Stage 10 — onboard WELL-14 via SDK
    add_well WELL-15 Field-East   # onboard a specific well

    demo_help              # print this help

EOF
}

echo "demo helpers loaded. Type: demo_help"
