#!/usr/bin/env python3
"""
build_kafka_header_enricher.py  (GENERIC / parameterized)
=========================================================
Builds and publishes the `kafka_header_enricher` pipeline fragment to Control Hub.

WHAT IT DOES (brief Section 4, generalised):
    Adds provenance HEADER attributes to every record so downstream Kafka
    consumers know where an event came from, without parsing the body:
      - entity_id        <- the record's unique-id field   (param ID_FIELD)
      - entity_group     <- the record's grouping field    (param GROUP_FIELD)
      - ingest_timestamp <- processing time (domain-agnostic)
      - pipeline_version <- static stamp     (domain-agnostic)

WHY IT'S GENERIC:
    The SOURCE field names are runtime parameters, so the same fragment serves
    every industry track. Each pipeline maps its own fields to the standard
    header names:
      - Oil & Gas:      ID_FIELD=well_id      GROUP_FIELD=field_name
      - Banking:        ID_FIELD=account_id   GROUP_FIELD=branch_id
      - Public sector:  ID_FIELD=citizen_id   GROUP_FIELD=agency
    Downstream consumers always read the same header names (entity_id,
    entity_group) regardless of domain - that consistency is the point.

SDK notes (verified against 6.6.2):
    - Expression Evaluator exposes header_attribute_expressions as a list of
      {attributeToSet, headerAttributeExpression} dicts.
    - Runtime parameters are set on the BUILT object via fragment.parameters.

Usage:
    python build_kafka_header_enricher.py --dry-run
    python build_kafka_header_enricher.py
"""
import argparse
import yaml
from streamsets.sdk import ControlHub

FRAGMENT_NAME = "kafka_header_enricher"
FRAGMENT_VERSION = "2.0.0"   # bumped from 1.0.0: now parameter-driven / generic
PIPELINE_VERSION_STAMP = "oil_gas-1.0.0"   # pipelines can override via param too

# Default source-field mapping (Oil & Gas). Pipelines override per-domain.
DEFAULT_PARAMETERS = {
    "ID_FIELD": "well_id",
    "GROUP_FIELD": "field_name",
    "PIPELINE_VERSION_STAMP": PIPELINE_VERSION_STAMP,
}


def load_config(path="sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="Build & publish generic kafka_header_enricher fragment")
    parser.add_argument("--dry-run", action="store_true", help="Build but do not publish")
    parser.add_argument("--config", default="sch_config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    ch, eng, pub = cfg["control_hub"], cfg["engine"], cfg["publish"]

    print(f"Connecting to Control Hub: {ch['url']}")
    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

    print(f"Building fragment '{FRAGMENT_NAME}' v{FRAGMENT_VERSION} (generic) ...")
    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"], fragment=True,
    )

    expr = builder.add_stage("Expression Evaluator")
    # Source fields come from runtime params; header names stay constant cross-domain.
    expr.header_attribute_expressions = [
        {
            "attributeToSet": "entity_id",
            "headerAttributeExpression": "${record:value(str:concat('/', '${ID_FIELD}'))}",
        },
        {
            "attributeToSet": "entity_group",
            "headerAttributeExpression": "${record:value(str:concat('/', '${GROUP_FIELD}'))}",
        },
        {
            "attributeToSet": "ingest_timestamp",
            "headerAttributeExpression": "${time:now()}",
        },
        {
            "attributeToSet": "pipeline_version",
            "headerAttributeExpression": "${PIPELINE_VERSION_STAMP}",
        },
    ]

    fragment = builder.build(
        title=FRAGMENT_NAME,
        description=(
            "Generic provenance header enricher. Writes entity_id, entity_group "
            "(from ID_FIELD / GROUP_FIELD runtime params), ingest_timestamp and "
            "pipeline_version as Kafka header attributes. Cross-domain: each "
            "pipeline maps its own source fields. KSEA shared fragment."
        ),
        labels=pub["labels"],
    )

    fragment.parameters = DEFAULT_PARAMETERS

    if args.dry_run:
        print("DRY RUN - fragment built but NOT published.")
        print(f"  title:      {FRAGMENT_NAME}  v{FRAGMENT_VERSION}")
        print(f"  parameters: {DEFAULT_PARAMETERS}")
        return

    print(f"Publishing to Control Hub (workspace: {pub['workspace']}) ...")
    sch.publish_pipeline(
        fragment,
        commit_message=f"{FRAGMENT_NAME} v{FRAGMENT_VERSION} - generic/parameterized (KSEA)",
    )
    print(f"Published '{FRAGMENT_NAME}' v{FRAGMENT_VERSION}. "
          f"Pipelines override ID_FIELD / GROUP_FIELD per domain.")


if __name__ == "__main__":
    main()
