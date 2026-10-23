#!/usr/bin/env python3
"""
wellhead_pipeline_manager.py — KSEA Oil & Gas pipeline manager (brief 5.1)
==========================================================================
Reads a YAML registry of all wellheads + their sensor topics, and generates the
MQTT ingestion pipeline in StreamSets Control Hub, applying the shared fragments
during construction. Supports onboarding a new wellhead from the CLI.

DESIGN DECISION (brief asks us to choose + justify):
  ONE PARAMETERISED PIPELINE, not one-per-well. Justification:
    * All 12 wells publish the SAME schema to topics sensors/wellhead/{well}/{metric}.
      A single MQTT subscription to the wildcard "sensors/wellhead/+/+" consumes
      every well and metric — and ANY new well — with no per-well duplication.
    * 12 separate pipelines would be 12x the build/publish/job/monitoring surface
      for identical logic: a maintenance and cost burden with no benefit.
    * Onboarding a new well therefore needs NO new pipeline — the wildcard already
      covers it. The manager's job is to keep the YAML registry (the documented
      inventory + per-well config) in sync and validate coverage, re-publishing
      the canonical pipeline so its embedded well list / labels stay current.
  This mirrors how the ingestion pipeline was actually built (topic_filter =
  ["sensors/wellhead/+/+"]).

What this manager does:
  generate     Build + publish the ingestion pipeline from the registry (applies
               kafka_header_enricher + data_quality_tagger fragments inline; DLQ
               inlined due to the SDK add_fragment S3 'displayFormats' bug).
  add-wellhead Add a new well to the registry YAML (and re-validate / re-generate).
  validate     Check the registry vs the running pipeline's topic coverage.
  list         Print the registry.

Usage:
  python wellhead_pipeline_manager.py generate
  python wellhead_pipeline_manager.py generate --dry-run
  python wellhead_pipeline_manager.py add-wellhead --well WELL-13 --field Field-West
  python wellhead_pipeline_manager.py add-wellhead --well WELL-13 --field Field-West --generate
  python wellhead_pipeline_manager.py validate
  python wellhead_pipeline_manager.py list

Deps: streamsets, pyyaml
"""
import argparse
import os
import sys
from pathlib import Path

import yaml
from streamsets.sdk import ControlHub

HERE = Path(__file__).resolve().parent
SCH_CONFIG = str(HERE.parent / "fragments" / "sch_config.yaml")
WELLHEADS_YAML = str(HERE.parent / "config" / "wellheads.yaml")
PIPELINE_NAME = "oil_gas__mqtt_ingestion"

# DLQ / MinIO config (matches the canonical ingestion build).
DLQ_BUCKET = "kermit-error-dlq"
DLQ_PREFIX = "oil_gas"
S3_ENDPOINT = "https://minio:9000"
S3_ACCESS_KEY = "kermit"
S3_SECRET_KEY = "kermit123"


def load_yaml(path):
    with open(path) as f:
        return yaml.safe_load(f)


def save_registry(reg, path=WELLHEADS_YAML):
    with open(path, "w") as f:
        yaml.safe_dump(reg, f, sort_keys=False, default_flow_style=False)


def connect():
    ch = load_yaml(SCH_CONFIG)["control_hub"]
    return ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])


# ---------------------------------------------------------------------------
# Pipeline construction (reuses the canonical ingestion build, registry-driven)
# ---------------------------------------------------------------------------
def build_ingestion_pipeline(sch, eng, registry, dry_run=False):
    wells = registry["wellheads"]
    wildcard = registry.get("topic_wildcard", "sensors/wellhead/+/+")

    print("Fetching shared fragments (enricher + tagger; DLQ inlined) ...")
    enricher = sch.pipelines.get(name="kafka_header_enricher", fragment=True)
    tagger = sch.pipelines.get(name="data_quality_tagger", fragment=True)
    print(f"  {enricher.name} v{enricher.version}")
    print(f"  {tagger.name} v{tagger.version}")

    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"],
    )

    mqtt = builder.add_stage("MQTT Subscriber", type="origin")
    mqtt.broker_url = "tcp://mosquitto:1883"
    mqtt.topic_filter = [wildcard]            # single wildcard covers all wells
    mqtt.quality_of_service = "AT_LEAST_ONCE"
    mqtt.data_format = "JSON"
    mqtt.buffer_size_in_bytes = 1048576

    header_frag = builder.add_fragment(enricher)
    quality_frag = builder.add_fragment(tagger)

    selector = builder.add_stage("Stream Selector", type="processor")

    dlq_expr = builder.add_stage("Expression Evaluator")
    dlq_expr.field_expressions = [
        {"fieldToSet": "/dlq_error_reason", "expression": "${record:value('/quality_status')}"},
        {"fieldToSet": "/dlq_failure_stage", "expression": "data_quality_tagger"},
        {"fieldToSet": "/dlq_source", "expression": PIPELINE_NAME},
        {"fieldToSet": "/dlq_captured_at", "expression": "${time:now()}"},
    ]

    dlq_s3 = builder.add_stage("Amazon S3", type="destination")
    dlq_s3.bucket = DLQ_BUCKET
    dlq_s3.common_prefix = DLQ_PREFIX
    dlq_s3.data_format = "JSON"
    dlq_s3.access_key_id = S3_ACCESS_KEY
    dlq_s3.secret_access_key = S3_SECRET_KEY
    dlq_s3.custom_endpoint_for_s3 = S3_ENDPOINT
    dlq_s3.use_path_style_address_model = True
    dlq_s3.region_definition_for_s3 = "SPECIFY_NON_REGIONAL_ENDPOINT"
    dlq_s3.checksum_algorithm = "SHA256"

    kafka = builder.add_stage("Kafka Producer", type="destination")
    kafka.broker_uri = "kafka:29092"
    kafka.topic = "sensors.normalised"
    kafka.data_format = "JSON"

    mqtt >> header_frag >> quality_frag >> selector

    quarantine_lane = selector.instance_name + "OutputLaneQuarantine"
    default_lane = selector.output_lanes[0]
    selector.condition = [
        {"outputLane": quarantine_lane, "predicate": "${record:value('/quality_score') == 0.0}"},
        {"outputLane": default_lane, "predicate": "default"},
    ]
    selector >> dlq_expr >> dlq_s3      # lane[0] quarantine -> DLQ
    selector >> kafka                   # lane[1] default    -> Kafka

    pipeline = builder.build(
        title=PIPELINE_NAME,
        description=(
            f"Oil & Gas MQTT ingestion (parameterised, registry-driven by "
            f"wellhead_pipeline_manager). Covers {len(wells)} wells via wildcard "
            f"'{wildcard}'. MQTT -> enricher -> quality tagger -> selector: "
            f"quarantine(0.0) -> S3/MinIO DLQ, clean -> sensors.normalised. KSEA."
        ),
        labels=["industry:oil_gas", "env:dev", "accelerator:ksea"],
    )

    if dry_run:
        print("DRY RUN — not published.")
        print(f"  wells covered: {len(wells)} via wildcard '{wildcard}'")
        print(f"  selector lanes: {selector.output_lanes}")
        return None

    sch.publish_pipeline(pipeline, commit_message=(
        f"{PIPELINE_NAME} — generated by wellhead_pipeline_manager "
        f"({len(wells)} wells) (KSEA)"
    ))
    print(f"Published '{PIPELINE_NAME}' covering {len(wells)} wells.")
    print("Recreate the job to run the new commit.")
    return pipeline


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_generate(args):
    registry = load_yaml(WELLHEADS_YAML)
    eng = load_yaml(SCH_CONFIG)["engine"]
    sch = connect()
    build_ingestion_pipeline(sch, eng, registry, dry_run=args.dry_run)


def cmd_add_wellhead(args):
    registry = load_yaml(WELLHEADS_YAML)
    well = args.well.upper()
    if well in registry["wellheads"]:
        print(f"{well} already in registry — nothing to add.")
    else:
        registry["wellheads"].append(well)
        registry["wellheads"].sort()
        if args.field:
            registry.setdefault("field_name_by_well", {})[well] = args.field
        save_registry(registry)
        print(f"Added {well}"
              + (f" (field {args.field})" if args.field else "")
              + f" to registry. Now {len(registry['wellheads'])} wells.")
        topics = [registry["topic_pattern"].format(well=well, metric=m)
                  for m in registry["metrics"]]
        print(f"  {well} will publish to: {', '.join(topics)}")
        print(f"  These match wildcard '{registry.get('topic_wildcard')}' — "
              f"NO new pipeline needed; the parameterised pipeline already covers it.")

    if args.generate:
        print("\nRe-generating the ingestion pipeline with the updated registry ...")
        eng = load_yaml(SCH_CONFIG)["engine"]
        sch = connect()
        build_ingestion_pipeline(sch, eng, registry, dry_run=args.dry_run)
    else:
        print("\n(Run 'generate' to re-publish the pipeline with the updated inventory,"
              " or pass --generate.)")


def cmd_validate(args):
    registry = load_yaml(WELLHEADS_YAML)
    wildcard = registry.get("topic_wildcard", "sensors/wellhead/+/+")
    print(f"Registry: {len(registry['wellheads'])} wells, {len(registry['metrics'])} metrics each.")
    print(f"Wildcard subscription '{wildcard}' covers pattern "
          f"'{registry['topic_pattern']}' for all wells.")
    # Confirm each well's topics match the wildcard shape
    ok = True
    for well in registry["wellheads"]:
        for m in registry["metrics"]:
            t = registry["topic_pattern"].format(well=well, metric=m)
            parts = t.split("/")
            if len(parts) != 4 or parts[0] != "sensors" or parts[1] != "wellhead":
                print(f"  WARNING: {t} does not match wildcard shape")
                ok = False
    print("All well topics match the wildcard — full coverage." if ok
          else "Some topics do NOT match — review topic_pattern.")
    # Optionally check the published pipeline exists
    try:
        sch = connect()
        pipes = [p for p in sch.pipelines if p.name == PIPELINE_NAME]
        if pipes:
            latest = sorted(pipes, key=lambda p: str(p.version))[-1]
            print(f"Published pipeline '{PIPELINE_NAME}' present (v{latest.version}).")
        else:
            print(f"No published pipeline named '{PIPELINE_NAME}' found — run 'generate'.")
    except Exception as e:
        print(f"(Could not check Control Hub: {e})")


def cmd_list(args):
    registry = load_yaml(WELLHEADS_YAML)
    fields = registry.get("field_name_by_well", {})
    print(f"Wellhead registry ({len(registry['wellheads'])} wells):")
    for w in registry["wellheads"]:
        print(f"  {w:<9} {fields.get(w, '—')}")
    print(f"Metrics: {', '.join(registry['metrics'])}")
    print(f"Wildcard: {registry.get('topic_wildcard')}")


def main():
    ap = argparse.ArgumentParser(description="Wellhead pipeline manager (brief 5.1)")
    sub = ap.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="build + publish ingestion pipeline from registry")
    g.add_argument("--dry-run", action="store_true")
    g.set_defaults(func=cmd_generate)

    a = sub.add_parser("add-wellhead", help="onboard a new wellhead into the registry")
    a.add_argument("--well", required=True, help="e.g. WELL-13")
    a.add_argument("--field", help="field label, e.g. Field-West")
    a.add_argument("--generate", action="store_true", help="re-publish pipeline after adding")
    a.add_argument("--dry-run", action="store_true")
    a.set_defaults(func=cmd_add_wellhead)

    v = sub.add_parser("validate", help="validate registry vs wildcard coverage")
    v.set_defaults(func=cmd_validate)

    l = sub.add_parser("list", help="print the registry")
    l.set_defaults(func=cmd_list)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
