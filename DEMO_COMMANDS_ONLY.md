# Oil & Gas Demo — Commands

## STAGE 0/1 — Source & Ingestion

```
docker exec oilgas-mosquitto mosquitto_sub -t 'sensors/wellhead/+/+' -C 5
```

Switch to: **StreamSets tab → ingestion pipeline canvas**

---

## STAGE 2 — Kafka

```
docker exec oilgas-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sensors.normalised --max-messages 3 --timeout-ms 10000
```

---

## STAGE 3 — Dead-Letter Queue

```
cd ~/Documents/oil-gas-acceleratorX && python wellhead_simulator.py --scenario dlq-demo
```

Ctrl+C after ~30 sec.

Switch to: **MinIO tab → `kermit-error-dlq` → `oil_gas/` folder → any object**

---

## STAGE 4 — MongoDB Archive

```
docker exec oilgas-mongodb mongosh -u kermit -p kermit --authenticationDatabase admin --quiet --eval "db=db.getSiblingDB('oilgas'); print('archive docs:', db.raw_sensor_events.countDocuments()); printjson(db.raw_sensor_events.findOne())"
```

---

## STAGE 5 — Anomaly Detection

Switch to: **StreamSets tab → anomaly detection pipeline canvas**

```
docker exec oilgas-postgres psql -U kermit -d oilgas -c "SELECT well_id, metric, value, severity, event_ts FROM anomaly_events ORDER BY id DESC LIMIT 8;"
```

---

## STAGE 6 — CRITICAL → Case

```
cd ~/Documents/oil-gas-acceleratorX && python wellhead_simulator.py --force-critical 7
```

Switch to: **Ops Console tab** (watch WELL-07 flash red)

Ctrl+C after ~30 sec, then:

```
docker exec oilgas-postgres psql -U kermit -d oilgas -c "SELECT case_id, well_id, anomaly_type, severity, recorded_at FROM maintenance_cases ORDER BY recorded_at DESC LIMIT 5;"
```

---

## STAGE 7 — Blowout Watchdog

```
tail -30 ~/oilgas-demo-logs/anomaly_watchdog.log | grep -E "BLOWOUT|NOTIFY|maintenance case"
```

Switch to: **Mailhog tab → refresh → "BLOWOUT: WELL-07" email**

---

## STAGE 8 — Shift Report

```
cd ~/Documents/oil-gas-acceleratorX && python scripts/shift_report_generator.py --shift morning
```

Switch to: **MinIO tab → `kermit-shift-reports` → new .xlsx**
Then: **Mailhog tab → refresh → shift report email**

---

## STAGE 9 — Monitoring

Switch to: **Grafana tab → dashboard**
Then: **Ops Console tab → recap**

---

## STAGE 10 — SDK: Onboard a New Well

```
cd ~/Documents/oil-gas-acceleratorX && python pipelines/wellhead_pipeline_manager.py list
```

```
python pipelines/wellhead_pipeline_manager.py add-wellhead --well WELL-14 --field Field-West
```

```
python pipelines/wellhead_pipeline_manager.py generate --dry-run
```

---

## AFTER RECORDING

```
cd ~/Documents/oil-gas-acceleratorX && ./stop_demo.sh
```

---

## Pre-run test (optional)

```
docker exec oilgas-postgres psql -U kermit -d oilgas -c "SELECT severity, COUNT(*) FROM anomaly_events GROUP BY severity;"
```

---

## Safety net

| Problem | Fix |
|---------|-----|
| Command errors | Say "let me retry" and run it again |
| Ops Console offline | `python ops_console/ops_dashboard_api.py` |
| Mailhog empty | Refresh the tab |
| MinIO shows empty bucket | Click into `oil_gas/` folder |
| Simulator exited | `python wellhead_simulator.py` |

---

## Tab order

**StreamSets → Ops Console → Mailhog → MinIO → Grafana → FastAPI docs**
