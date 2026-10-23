#!/usr/bin/env python3
"""
inspect_mqtt_kafka.py
---------------------
Confirms the MQTT origin + Kafka destination stage labels and key attribute
names on THIS engine, so the ingestion pipeline wires correctly first time.
"""
import yaml
from streamsets.sdk import ControlHub

cfg = yaml.safe_load(open("sch_config.yaml"))
ch, eng = cfg["control_hub"], cfg["engine"]
sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

def probe(label, stype):
    try:
        b = sch.get_pipeline_builder(engine_type=eng["engine_type"], engine_id=eng["engine_id"])
        b.add_stage(label, type=stype)
        print(f"PASS  '{label}' ({stype})")
        return True
    except Exception as e:
        print(f"----  '{label}' ({stype}): {type(e).__name__}")
        return False

print("=== MQTT origin candidates ===")
for lbl in ["MQTT Subscriber", "MQTT Consumer"]:
    probe(lbl, "origin")

print("\n=== Kafka destination candidates ===")
for lbl in ["Kafka Producer", "Kafka"]:
    probe(lbl, "destination")

# Dump key attrs for the ones that exist
builder = sch.get_pipeline_builder(engine_type=eng["engine_type"], engine_id=eng["engine_id"])
for lbl, stype in [("MQTT Subscriber", "origin"), ("Kafka Producer", "destination")]:
    try:
        b = sch.get_pipeline_builder(engine_type=eng["engine_type"], engine_id=eng["engine_id"])
        s = b.add_stage(lbl, type=stype)
        attrs = [a for a in dir(s) if not a.startswith("_") and not callable(getattr(s, a, None))]
        print(f"\n=== Key attributes on '{lbl}' ===")
        for a in sorted(attrs):
            if any(k in a.lower() for k in ("broker","topic","url","mqtt","quality","qos",
                                             "data_format","client","bootstrap","kafka","key")):
                try:
                    print(f"  {a:38} = {getattr(s, a)!r}")
                except Exception:
                    print(f"  {a:38} = <unreadable>")
    except Exception as e:
        print(f"\n('{lbl}' not available: {e})")
