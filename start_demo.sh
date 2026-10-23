#!/usr/bin/env bash
# =====================================================================
# start_demo.sh — one-command demo setup.
#
# Starts (in the background) everything you need for a live demo:
#   - Mongo archiver
#   - Ops Console API
#   - Blowout watchdog (continuous)
#   - Simulator (default mode, calm baseline)
#
# Then waits for data to accumulate and prints READY / NOT READY.
#
# Usage:
#   ./start_demo.sh
#
# Sibling script: stop_demo.sh (kills everything this started).
# Log files: ~/oilgas-demo-logs/
# =====================================================================
set -u

PROJECT="${PROJECT:-$(cd "$(dirname "$0")" && pwd)}"
VENV="$PROJECT/.venv"
LOGDIR="$HOME/oilgas-demo-logs"
PIDFILE="$LOGDIR/pids"

# ---------- colors ----------
C_OK='\033[0;32m'; C_WARN='\033[0;33m'; C_ERR='\033[0;31m'; C_DIM='\033[2m'; C_RESET='\033[0m'
ok()   { printf "${C_OK}✓${C_RESET}  %s\n" "$1"; }
warn() { printf "${C_WARN}!${C_RESET}  %s\n" "$1"; }
err()  { printf "${C_ERR}✗${C_RESET}  %s\n" "$1"; }
step() { printf "\n${C_DIM}--${C_RESET} %s\n" "$1"; }

mkdir -p "$LOGDIR"
: > "$PIDFILE"

# ---------- 0. sanity checks ----------
step "sanity checks"
[ -d "$PROJECT" ] || { err "project dir not found: $PROJECT"; exit 1; }
[ -f "$VENV/bin/activate" ] || { err "venv not found: $VENV"; exit 1; }

# shellcheck disable=SC1091
source "$VENV/bin/activate"
cd "$PROJECT" || exit 1

# Check docker containers we need
need_containers=(oilgas-postgres oilgas-mongodb oilgas-kafka oilgas-mosquitto oilgas-minio oilgas-mailhog oilgas-maintenance-api oilgas-grafana amazing_benz)
missing=0
for c in "${need_containers[@]}"; do
  if docker ps --format '{{.Names}}' | grep -q "^${c}\$"; then
    ok "container up: $c"
  else
    err "container NOT running: $c"; missing=$((missing+1))
  fi
done
[ "$missing" -gt 0 ] && { err "start missing containers before running this script"; exit 1; }

# ---------- 1. remind about SS jobs ----------
step "StreamSets jobs"
warn "Make sure BOTH jobs are ACTIVE in Control Hub:"
echo "     - Job for oil_gas__mqtt_ingestion"
echo "     - Job for oil_gas__anomaly_detection"
warn "This script does NOT start them (needs Control Hub credentials). Do it manually."
read -r -p "  Press ENTER when both jobs show ACTIVE (or Ctrl+C to abort)..." _

# ---------- helper to launch background processes ----------
launch() {
  local name="$1"; shift
  local logfile="$LOGDIR/${name}.log"
  # shellcheck disable=SC2068
  ( cd "$PROJECT" && $@ ) > "$logfile" 2>&1 &
  local pid=$!
  echo "${name}=${pid}" >> "$PIDFILE"
  sleep 0.5
  if kill -0 "$pid" 2>/dev/null; then
    ok "started $name  (pid $pid, log: $logfile)"
  else
    err "$name FAILED to start — see $logfile"
    tail -5 "$logfile" | sed 's/^/     /'
  fi
}

# ---------- 2. start background processes ----------
step "starting background services"
launch mongo_archiver     python scripts/mongo_archiver.py
launch ops_console_api    python ops_console/ops_dashboard_api.py
launch anomaly_watchdog   python scripts/anomaly_watchdog.py --pause-mode soft
launch simulator          python wellhead_simulator.py

# ---------- 3. wait for baseline data ----------
step "waiting 90s for baseline data to accumulate"
for i in $(seq 90 -10 10); do
  printf "  ${C_DIM}%ds remaining...${C_RESET}\r" "$i"
  sleep 10
done
echo

# ---------- 4. verify data flow ----------
step "verification"

pg_query() {
  docker exec oilgas-postgres psql -U kermit -d oilgas -tAc "$1" 2>/dev/null | tr -d ' '
}

anomaly_rows=$(pg_query "SELECT COUNT(*) FROM anomaly_events;")
audit_rows=$(pg_query "SELECT COUNT(*) FROM audit_log;")
mongo_docs=$(docker exec oilgas-mongodb mongosh -u kermit -p kermit --authenticationDatabase admin \
  --quiet --eval "db=db.getSiblingDB('oilgas'); print(db.raw_sensor_events.countDocuments())" 2>/dev/null | tail -1)

echo "  anomaly_events rows:  $anomaly_rows"
echo "  audit_log rows:       $audit_rows"
echo "  mongo archive docs:   $mongo_docs"

# ---------- 5. verdict ----------
step "verdict"
ready=1
if [ "${anomaly_rows:-0}" -lt 3 ]; then
  warn "anomaly_events looks low — default mode may need more time, or the anomaly job isn't ACTIVE"
  ready=0
fi
if [ "${mongo_docs:-0}" -lt 100 ]; then
  warn "mongo archive looks low — check mongo_archiver.log"
  ready=0
fi

if [ "$ready" -eq 1 ]; then
  ok "READY TO RECORD ✓"
  echo
  echo "  Browser tabs to open:"
  echo "    Ops Console      file://$PROJECT/ops_console/ops_console.html"
  echo "    Grafana          http://localhost:3001"
  echo "    Mailhog          http://localhost:8025"
  echo "    MinIO console    http://localhost:9090       (kermit / kermit123)"
  echo "    Maintenance API  http://localhost:8000/docs"
  echo "    StreamSets       https://eu02.hub.streamsets.com"
  echo
  echo "  Load demo helpers in your recording terminal with:"
  echo "    source ./demo_helpers.sh"
  echo
  echo "  To stop everything after recording:"
  echo "    ./stop_demo.sh"
else
  err "NOT READY — investigate the warnings above before recording."
  echo "  Logs in $LOGDIR/"
fi
