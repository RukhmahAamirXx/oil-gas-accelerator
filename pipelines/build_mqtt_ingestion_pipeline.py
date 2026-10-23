#!/usr/bin/env python3
"""
build_mqtt_ingestion_pipeline.py  (STEP 4 - stream selector + INLINED DLQ)
==========================================================================
Full Oil & Gas MQTT ingestion pipeline:

    MQTT >> kafka_header_enricher >> data_quality_tagger >> Stream Selector
                  |-- score==0.0 --> [DLQ: Expression Eval >> Amazon S3/MinIO]
                  `-- default ------> Kafka Producer (sensors.normalised)

WHY THE DLQ IS INLINED (not add_fragment):
    The StreamSets SDK's builder.add_fragment() throws KeyError 'displayFormats'
    on the dead_letter_queue_handler fragment because its Amazon S3 stage carries
    a config the SDK's parameter-remapper can't handle. The header enricher and
    quality tagger (Expression Evaluators) add fine; only the S3-containing
    fragment breaks. So the DLQ's two stages (error-envelope Expression Evaluator
    + Amazon S3 -> MinIO) are built INLINE here using the exact same proven
    MinIO wiring (path-style, non-regional https endpoint, SHA256 checksum) from
    dead_letter_queue_handler v2.0.0. Functionally identical; avoids the SDK bug.

Test:  python wellhead_simulator.py --scenario dlq-demo
       -> well 9 null readings score 0.0 -> DLQ/MinIO ; everyone else -> Kafka

Usage:
    python build_mqtt_ingestion_pipeline.py --dry-run
    python build_mqtt_ingestion_pipeline.py
"""
import argparse
import yaml
from streamsets.sdk import ControlHub

PIPELINE_NAME = "oil_gas__mqtt_ingestion"

# DLQ / MinIO connection (matches dead_letter_queue_handler v2.0.0 exactly)
DLQ_BUCKET = "kermit-error-dlq"
DLQ_PREFIX = "oil_gas"
S3_ENDPOINT = "https://minio:9000"
S3_ACCESS_KEY = "kermit"
S3_SECRET_KEY = "kermit123"


def load_config(path="../fragments/sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--config", default="../fragments/sch_config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ch, eng, pub = cfg["control_hub"], cfg["engine"], cfg["publish"]

    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

    print("Fetching fragments (enricher + tagger only; DLQ is inlined) ...")
    enricher = sch.pipelines.get(name="kafka_header_enricher", fragment=True)
    tagger = sch.pipelines.get(name="data_quality_tagger", fragment=True)
    print(f"  {enricher.name} v{enricher.version}")
    print(f"  {tagger.name} v{tagger.version}")

    print(f"Building pipeline '{PIPELINE_NAME}' (step 4: stream selector + inlined DLQ) ...")
    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"],
    )

    mqtt = builder.add_stage("MQTT Subscriber", type="origin")
    mqtt.broker_url = "tcp://mosquitto:1883"
    mqtt.topic_filter = ["sensors/wellhead/+/+"]
    mqtt.quality_of_service = "AT_LEAST_ONCE"
    mqtt.data_format = "JSON"
    mqtt.buffer_size_in_bytes = 1048576

    header_frag = builder.add_fragment(enricher)
    quality_frag = builder.add_fragment(tagger)

    selector = builder.add_stage("Stream Selector", type="processor")

    # --- Inlined DLQ branch: Expression Evaluator (error envelope) -> Amazon S3/MinIO ---
    dlq_expr = builder.add_stage("Expression Evaluator")
    dlq_expr.field_expressions = [
        {"fieldToSet": "/dlq_error_reason",
         "expression": "${record:value('/quality_status')}"},
        {"fieldToSet": "/dlq_failure_stage", "expression": "data_quality_tagger"},
        {"fieldToSet": "/dlq_source",        "expression": "oil_gas__mqtt_ingestion"},
        {"fieldToSet": "/dlq_captured_at",   "expression": "${time:now()}"},
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

    # --- Clean branch: Kafka ---
    kafka = builder.add_stage("Kafka Producer", type="destination")
    kafka.broker_uri = "kafka:29092"
    kafka.topic = "sensors.normalised"
    kafka.data_format = "JSON"

    # Spine into selector
    mqtt >> header_frag >> quality_frag >> selector

    # Stream Selector lanes are driven by PREDICATES (variable_output_drive =
    # 'lanePredicates'), NOT by connections. Setting `condition` with two
    # predicates - the first referencing a NEW lane id - creates the second lane.
    # (Proven empirically: connections never grow output_lanes; predicates do.)
    quarantine_lane = selector.instance_name + "OutputLaneQuarantine"
    default_lane = selector.output_lanes[0]   # the pre-existing lane
    selector.condition = [
        {"outputLane": quarantine_lane,
         "predicate": "${record:value('/quality_score') == 0.0}"},
        {"outputLane": default_lane,
         "predicate": "default"},
    ]

    # Now both lanes exist in order: [0]=quarantine, [1]=default.
    # Wire each lane to its branch. Connecting to a stage uses the next lane.
    selector >> dlq_expr >> dlq_s3      # lane[0] (quarantine) -> DLQ
    selector >> kafka                   # lane[1] (default)    -> Kafka

    pipeline = builder.build(
        title=PIPELINE_NAME,
        description=(
            "Oil & Gas MQTT ingestion (step 4 - full, inlined DLQ). MQTT -> header "
            "enricher -> quality tagger -> stream selector: quarantined (0.0) -> "
            "Expression+S3/MinIO DLQ, clean -> sensors.normalised. DLQ inlined due to "
            "SDK add_fragment S3 bug. KSEA."
        ),
        labels=pub["labels"],
    )

    if args.dry_run:
        print("DRY RUN - not published.")
        print(f"  selector lanes: {selector.output_lanes}")
        print("  lane[0] (score==0.0) -> DLQ Expression -> S3/MinIO")
        print("  lane[1] (default)    -> Kafka")
        return

    sch.publish_pipeline(
        pipeline,
        commit_message=f"{PIPELINE_NAME} - step4 selector + inlined DLQ (KSEA)",
    )
    print(f"Published '{PIPELINE_NAME}'.")
    print("Recreate the job. Test: python wellhead_simulator.py --scenario dlq-demo")


if __name__ == "__main__":
    main()
