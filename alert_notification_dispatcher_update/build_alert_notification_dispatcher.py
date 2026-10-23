#!/usr/bin/env python3
"""
build_alert_notification_dispatcher.py  (GENERIC / parameterized)
=================================================================
Builds and publishes the `alert_notification_dispatcher` pipeline fragment.

WHAT IT DOES (brief Section 4):
    Dispatches alerts for classified records and, for CRITICAL severity, calls
    the maintenance case API with retry + exponential backoff.

IMPORTANT SCOPING NOTE (be ready to say this at sign-off):
    The brief's "CRITICAL = repeated breaches on the same well within 10 minutes"
    is STATEFUL detection - it needs memory across records. A fragment processes
    one record at a time and cannot hold that state, so that detection correctly
    lives in anomaly_watchdog.py (Day 8), NOT in this fragment.

    This fragment's job is the DISPATCH half: given a record already classified
    (severity field set upstream by the anomaly-detection logic), it POSTs
    CRITICAL records to the maintenance API. WARN vs CRITICAL routing is done by
    the pipeline's Stream Selector feeding this fragment.

STRUCTURE:
    HTTP Client (destination) -> POST to ${MAINTENANCE_API_URL}/cases
      - retries:            ${API_MAX_RETRIES}  (default 3)
      - backoff:            exponential (configured on the stage)
    Endpoint and retry counts are RUNTIME PARAMETERS so any team member points
    it at their own API.

WHY IT'S GENERIC:
    Nothing oil-and-gas specific. Any domain that needs "on CRITICAL, open a
    case via an API with retries" uses this unchanged, overriding the URL.

SDK notes (verified): HTTP Client stage label is "HTTP Client" on this engine.
    HTTP retry/backoff config attribute names vary by version; verify on engine.

Usage:
    python build_alert_notification_dispatcher.py --dry-run
    python build_alert_notification_dispatcher.py
"""
import argparse
import yaml
from streamsets.sdk import ControlHub

FRAGMENT_NAME = "alert_notification_dispatcher"
FRAGMENT_VERSION = "3.0.0"

DEFAULT_PARAMETERS = {
    "MAINTENANCE_API_URL": "http://maintenance-api:8000/cases",
    "API_MAX_RETRIES": "3",
    "API_BACKOFF_BASE_MS": "1000",   # 1s, doubling each retry (exponential)
    # --- field-mapping params (make the shaper generic / no-modification reuse) ---
    # ID_FIELD     : the record's identifier field  (Oil&Gas: well_id, Mfg: machine_id, ...)
    # ID_OUT_KEY   : the key the maintenance API's CaseRequest expects for that id
    # METRIC_FIELD : the record's metric/anomaly-type source field
    "ID_FIELD": "well_id",
    "ID_OUT_KEY": "well_id",
    "METRIC_FIELD": "metric",
}


def load_config(path="sch_config.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def main():
    ap = argparse.ArgumentParser(description="Build & publish alert_notification_dispatcher fragment")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--config", default="sch_config.yaml")
    args = ap.parse_args()

    cfg = load_config(args.config)
    ch, eng, pub = cfg["control_hub"], cfg["engine"], cfg["publish"]

    print(f"Connecting to Control Hub: {ch['url']}")
    sch = ControlHub(credential_id=ch["credential_id"], token=ch["credential_token"])

    print(f"Building fragment '{FRAGMENT_NAME}' v{FRAGMENT_VERSION} ...")
    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"], fragment=True,
    )

    # JavaScript Evaluator: shape the record into the EXACT CaseRequest the
    # maintenance API expects - 4 string fields: well_id, anomaly_type, severity,
    # timestamp. The incoming record has ~11 fields (and timestamp may be a
    # DATETIME); a strict (FastAPI/Pydantic) API rejects extra/typed fields with
    # 422. We REPLACE the record value with just these 4 string fields.
    shaper = builder.add_stage("JavaScript Evaluator")
    # GENERIC payload shaper. The identifier field name, its output key, and the
    # metric field are RUNTIME PARAMETERS (${ID_FIELD}, ${ID_OUT_KEY},
    # ${METRIC_FIELD}) substituted into the script by the engine. So every team
    # member uses this fragment UNCHANGED - they only set params on their pipeline
    # (Oil&Gas: ID_FIELD=well_id; Manufacturing: ID_FIELD=machine_id; etc.).
    # severity + timestamp are standard across the accelerator's classified records.
    shaper.script = """
for (var i = 0; i < records.length; i++) {
  try {
    var r = records[i];
    var idVal  = r.value['${ID_FIELD}'];
    var metric = r.value['${METRIC_FIELD}'];
    var sev    = r.value['severity'];
    var ts     = r.value['timestamp'];
    // timestamp may be a Date (DATETIME) or string; coerce to ISO string.
    var tsStr;
    if (ts == null) { tsStr = ''; }
    else if (ts instanceof Date) { tsStr = ts.toISOString(); }
    else { tsStr = '' + ts; }
    // Replace the record value with ONLY the fields the case API expects.
    // The id is emitted under the API's expected key (${ID_OUT_KEY}).
    var out = {};
    out['${ID_OUT_KEY}']  = idVal == null ? '' : '' + idVal;
    out['anomaly_type']   = metric == null ? '' : '' + metric;
    out['severity']       = sev == null ? '' : '' + sev;
    out['timestamp']      = tsStr;
    r.value = out;
    output.write(r);
  } catch (e) {
    error.write(records[i], 'alert payload shaping error: ' + e);
  }
}
"""

    # HTTP Client destination -> POST one clean CaseRequest per record.
    http = builder.add_stage("HTTP Client", type="destination")
    try:
        http.resource_url = "${MAINTENANCE_API_URL}"
        http.http_method = "POST"
        http.data_format = "JSON"
        http.one_request_per_batch = False        # ONE request per record (fixes 422 "Extra data")
        http.json_content = "MULTIPLE_OBJECTS"     # single object body per request
        http.maximum_retries = "${API_MAX_RETRIES}"
    except Exception:
        pass

    # Wire: shaper -> http
    shaper >> http

    fragment = builder.build(
        title=FRAGMENT_NAME,
        description=(
            "Dispatches CRITICAL records to the maintenance case API via HTTP "
            "POST with retry + exponential backoff (URL/retries are runtime "
            "params). Stateful repeated-breach detection lives in the watchdog, "
            "not here. KSEA shared fragment."
        ),
        labels=pub["labels"],
    )
    fragment.parameters = DEFAULT_PARAMETERS

    if args.dry_run:
        print("DRY RUN - fragment built but NOT published.")
        print(f"  title:      {FRAGMENT_NAME} v{FRAGMENT_VERSION}")
        print(f"  parameters: {DEFAULT_PARAMETERS}")
        return

    print(f"Publishing to Control Hub (workspace: {pub['workspace']}) ...")
    sch.publish_pipeline(
        fragment,
        commit_message=f"{FRAGMENT_NAME} v{FRAGMENT_VERSION} - initial publish (KSEA)",
    )
    print(f"Published '{FRAGMENT_NAME}'.")


if __name__ == "__main__":
    main()
