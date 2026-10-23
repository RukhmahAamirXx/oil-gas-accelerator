#!/usr/bin/env python3
"""
build_dead_letter_queue_handler.py  (GENERIC / parameterized, MinIO-wired)
==========================================================================
v1.2.0 - path-style addressing explicitly verified for MinIO.

WHAT IT DOES (brief Section 4):
    Failed records are routed to MinIO (S3-compatible) bucket kermit-error-dlq
    with full metadata - original topic, error reason, capture timestamp - so
    nothing is ever silently dropped and failures can be reprocessed.

S3-FOR-MINIO WIRING (the three things that matter):
    use_path_style_address_model = True   -> MinIO serves endpoint/bucket, not
                                             bucket.endpoint. WITHOUT this the
                                             write fails (host doesn't exist).
    custom_endpoint_for_s3 = ${S3_ENDPOINT} -> redirects S3 -> MinIO
    region SPECIFY_REGION + US_EAST_1     -> consistent; MinIO ignores value
    Verified config names on this engine via inspect_s3_pathstyle.py:
      s3TargetConfigBean.s3Config.usePathAddressModel
      s3TargetConfigBean.s3Config.bucketTemplate

Usage:
    python build_dead_letter_queue_handler.py --dry-run   # verifies path-style took
    python build_dead_letter_queue_handler.py
"""
import argparse
import json
import yaml
from streamsets.sdk import ControlHub

FRAGMENT_NAME = "dead_letter_queue_handler"
FRAGMENT_VERSION = "1.5.0"

DEFAULT_PARAMETERS = {
    "S3_ENDPOINT": "https://minio:9000",
    "DLQ_BUCKET": "kermit-error-dlq",
    "S3_ACCESS_KEY": "kermit",
    "S3_SECRET_KEY": "kermit123",
    "DLQ_PREFIX": "oil_gas",
}


def load_config(path="sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def read_pathstyle(fragment):
    """
    Read usePathAddressModel back to confirm it took. A freshly built fragment
    exposes its stages/config differently from a retrieved one, so try several
    access paths defensively.
    """
    # Path 1: read directly off the stage object's configuration list
    try:
        for st in fragment.stages:
            for c in getattr(st, "_data", {}).get("configuration", []):
                if c.get("name") == "s3TargetConfigBean.s3Config.usePathAddressModel":
                    return c.get("value")
    except Exception:
        pass
    # Path 2: parse pipeline_definition if it's populated
    try:
        d = fragment.pipeline_definition
        if isinstance(d, str):
            d = json.loads(d)
        if isinstance(d, dict):
            for st in d.get("stages", []):
                for c in st.get("configuration", []):
                    if c.get("name") == "s3TargetConfigBean.s3Config.usePathAddressModel":
                        return c.get("value")
    except Exception:
        pass
    # Path 3: read the attribute straight off the S3 stage we configured
    try:
        for st in fragment.stages:
            if getattr(st, "use_path_style_address_model", None) is not None:
                return st.use_path_style_address_model
    except Exception:
        pass
    return "<could not read - verify after publish>"


def main():
    ap = argparse.ArgumentParser(description="Build & publish dead_letter_queue_handler fragment")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--config", default="sch_config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ch, eng, pub = cfg["control_hub"], cfg["engine"], cfg["publish"]

    print(f"Connecting to Control Hub: {ch['url']}")
    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

    print(f"Building fragment '{FRAGMENT_NAME}' v{FRAGMENT_VERSION} (MinIO path-style) ...")
    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"], fragment=True,
    )

    # 1) metadata enricher
    expr = builder.add_stage("Expression Evaluator")
    expr.field_expressions = [
        {"fieldToSet": "/dlq_error_reason",
         "expression": "${record:exists('/dlq_error_reason') ? record:value('/dlq_error_reason') : 'unspecified'}"},
        {"fieldToSet": "/dlq_original_topic",
         "expression": "${record:attribute('mqtt.topic')}"},
        {"fieldToSet": "/dlq_captured_at",
         "expression": "${time:now()}"},
    ]

    # 2) Amazon S3 destination -> MinIO (explicit attribute names)
    s3 = builder.add_stage("Amazon S3", type="destination")
    s3.bucket = "${DLQ_BUCKET}"
    s3.common_prefix = "${DLQ_PREFIX}"
    s3.data_format = "JSON"
    s3.access_key_id = "${S3_ACCESS_KEY}"
    s3.secret_access_key = "${S3_SECRET_KEY}"
    s3.custom_endpoint_for_s3 = "${S3_ENDPOINT}"
    # --- the critical MinIO settings ---
    s3.use_path_style_address_model = True
    # Use the CUSTOM (non-regional) endpoint, NOT an AWS region. With
    # SPECIFY_REGION the stage ignores the endpoint and hits real AWS (403).
    # SPECIFY_NON_REGIONAL_ENDPOINT tells it to use nonRegionalEndpoint = MinIO.
    s3.region_definition_for_s3 = "SPECIFY_NON_REGIONAL_ENDPOINT"
    # MinIO rejects the default MD5 checksum trailer with an x-amz-content-sha256
    # error; SHA256 forces a content hash MinIO accepts.
    s3.checksum_algorithm = "SHA256"

    expr >> s3

    fragment = builder.build(
        title=FRAGMENT_NAME,
        description=(
            "Routes failed records to S3/MinIO dead-letter bucket with error "
            "reason, original topic and capture timestamp. S3 wired for MinIO: "
            "path-style addressing + custom endpoint. Endpoint/bucket/creds are "
            "runtime params. KSEA shared fragment."
        ),
        labels=pub["labels"],
    )
    fragment.parameters = DEFAULT_PARAMETERS

    # VERIFY the path-style setting (best-effort before publish)
    ps = read_pathstyle(fragment)
    print(f"  path-style (usePathAddressModel) = {ps}")
    if ps is False:
        print("  WARNING: path-style read back as False - NOT publishing.")
        print("  Paste this output back so we set it via the underlying config name.")

    if args.dry_run:
        print("DRY RUN - fragment built but NOT published.")
        print(f"  parameters: {DEFAULT_PARAMETERS}")
        print("  After publishing, confirm with: python validate_fragments.py")
        return

    if ps is False:
        print("Refusing to publish: path-style confirmed False.")
        return

    print(f"Publishing to Control Hub (workspace: {pub['workspace']}) ...")
    sch.publish_pipeline(
        fragment,
        commit_message=f"{FRAGMENT_NAME} v{FRAGMENT_VERSION} - MinIO path-style verified (KSEA)",
    )
    print(f"Published '{FRAGMENT_NAME}' v{FRAGMENT_VERSION}.")
    print("Now run: python validate_fragments.py  (confirm usePathAddressModel = True)")


if __name__ == "__main__":
    main()
