#!/usr/bin/env bash
# stop_demo.sh — stop everything start_demo.sh started.

LOGDIR="$HOME/oilgas-demo-logs"
PIDFILE="$LOGDIR/pids"

[ -f "$PIDFILE" ] || { echo "no pidfile ($PIDFILE), nothing to stop"; exit 0; }

echo "stopping background demo processes..."
while IFS='=' read -r name pid; do
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" && echo "  stopped $name (pid $pid)"
  else
    echo "  $name (pid $pid) already gone"
  fi
done < "$PIDFILE"
rm -f "$PIDFILE"
echo "done. StreamSets jobs left running — stop them in Control Hub if needed."
