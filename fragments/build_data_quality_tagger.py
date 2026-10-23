#!/usr/bin/env python3
"""
build_data_quality_tagger.py  (GENERIC, build-time field config) - v3.0.0
=========================================================================
Tags each record with a quality score based on whether configured required
fields are present and non-null.

WHY v3.0.0 CHANGED THE APPROACH:
    Earlier versions tried to drive the required-field NAMES via RUNTIME
    parameters, building the field path with str:concat('/', ${PARAM}) or
    '/${PARAM}'. PROVEN (via a minimal hardcoded test) that StreamSets EL does
    NOT resolve a parameter-substituted/concatenated string as a field path -
    record:exists() returned false for a field that demonstrably existed, so
    every record was wrongly QUARANTINED.
    A hardcoded literal path - record:exists('/value') - works perfectly.

    FIX: build the LITERAL field paths in Python at fragment-build time. The
    published fragment contains record:exists('/value') etc. directly. Each
    track stays generic by building with its own REQUIRED_FIELDS list below
    (build-time configuration instead of runtime params).

OIL & GAS (narrow per-metric telemetry {well_id, field_name, timestamp,
metric, value}) requires: value, metric.
Other tracks: change REQUIRED_FIELDS and rebuild.

Sets:
    /quality_score   1.0 if all required fields present & non-null, else 0.0
    /quality_status  OK / QUARANTINE
    /quality_checked_at

Usage:
    python build_data_quality_tagger.py --dry-run
    python build_data_quality_tagger.py
"""
import argparse
import yaml
from streamsets.sdk import ControlHub

FRAGMENT_NAME = "data_quality_tagger"
FRAGMENT_VERSION = "3.0.0"

# Build-time required fields for THIS track (oil & gas narrow format).
# A record is QUARANTINE if ANY of these is missing or null.
REQUIRED_FIELDS = ["value", "metric"]


def load_config(path="sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def build_quality_expr(required_fields):
    """
    Build the score expression with LITERAL paths (resolved at build time).
    A field is BAD if not present OR its value is null.
    Score 0.0 if any field bad, else 1.0.
    """
    bad_clauses = []
    for f in required_fields:
        path = f"'/{f}'"   # literal '/value', '/metric', ...
        bad_clauses.append(
            f"(not record:exists({path}) or record:value({path}) == null)"
        )
    any_bad = " or ".join(bad_clauses)
    return f"${{({any_bad}) ? 0.0 : 1.0}}"


def build_status_expr(required_fields):
    bad_clauses = []
    for f in required_fields:
        path = f"'/{f}'"
        bad_clauses.append(
            f"(not record:exists({path}) or record:value({path}) == null)"
        )
    any_bad = " or ".join(bad_clauses)
    return f"${{({any_bad}) ? 'QUARANTINE' : 'OK'}}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--config", default="sch_config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ch, eng, pub = cfg["control_hub"], cfg["engine"], cfg["publish"]

    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])
    print(f"Building '{FRAGMENT_NAME}' v{FRAGMENT_VERSION} for required fields: {REQUIRED_FIELDS}")

    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"], fragment=True,
    )

    score_expr = build_quality_expr(REQUIRED_FIELDS)
    status_expr = build_status_expr(REQUIRED_FIELDS)
    print("  score expression:")
    print(f"    {score_expr}")

    expr = builder.add_stage("Expression Evaluator")
    expr.field_expressions = [
        {"fieldToSet": "/quality_score",      "expression": score_expr},
        {"fieldToSet": "/quality_status",     "expression": status_expr},
        {"fieldToSet": "/quality_checked_at", "expression": "${time:now()}"},
    ]

    fragment = builder.build(
        title=FRAGMENT_NAME,
        description=(
            "Tags records with quality_score/status based on presence of required "
            f"fields ({', '.join(REQUIRED_FIELDS)} for this track). Literal field "
            "paths baked at build time (runtime-param paths don't resolve in EL). "
            "Generic via build-time REQUIRED_FIELDS per track. KSEA shared fragment."
        ),
        labels=pub["labels"],
    )

    if args.dry_run:
        print("DRY RUN - not published.")
        return

    sch.publish_pipeline(
        fragment,
        commit_message=f"{FRAGMENT_NAME} v{FRAGMENT_VERSION} - literal-path quality check ({'/'.join(REQUIRED_FIELDS)}) (KSEA)",
    )
    print(f"Published '{FRAGMENT_NAME}' v{FRAGMENT_VERSION}.")


if __name__ == "__main__":
    main()
