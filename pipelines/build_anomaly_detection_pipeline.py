#!/usr/bin/env python3
"""
build_anomaly_detection_pipeline.py  (STEP 4b - + CRITICAL alert dispatch)
=======================================================================
    Kafka Multitopic Consumer (sensors.normalised)
        >> JavaScript Evaluator (severity classifier)
        >> Trash

The classifier reads each record's /metric and /value, looks up the threshold
bands (embedded below, copied verbatim from config/thresholds.yaml), and sets:
    /severity        NORMAL | WARN | CRITICAL
    /severity_reason short text explaining the classification
    /classified_at   epoch ms

Classification model (from thresholds.yaml):
  numeric:  CRITICAL if value < critical.low or > critical.high   (checked first)
            WARN     if value < warn.low or > warn.high
            else NORMAL
  categorical (valve_status): membership - FAULT=CRITICAL, PARTIAL=WARN,
            OPEN/CLOSED=NORMAL, anything else=WARN (unexpected)
  null/missing value on a numeric metric -> CRITICAL (data-quality)

Engine note: Jython/Groovy unavailable; using JavaScript Evaluator.

Usage:
    python build_anomaly_detection_pipeline.py --dry-run
    python build_anomaly_detection_pipeline.py
"""
import argparse
import json
import yaml
from streamsets.sdk import ControlHub

PIPELINE_NAME = "oil_gas__anomaly_detection"

# Thresholds copied verbatim from config/thresholds.yaml - KEEP IN SYNC.
THRESHOLDS = {
    "pressure_bar":  {"type": "numeric", "normal": [150, 280], "warn": [140, 290], "critical": [120, 310]},
    "temp_celsius":  {"type": "numeric", "normal": [60, 90],   "warn": [55, 95],   "critical": [45, 105]},
    "flow_rate_m3h": {"type": "numeric", "normal": [800, 1200],"warn": [750, 1250],"critical": [600, 1400]},
    "vibration_g":   {"type": "numeric", "normal": [0, 2.5],   "warn": [-1, 3.0],  "critical": [-1, 4.5]},
    "valve_status":  {"type": "categorical", "normal": ["OPEN", "CLOSED"], "warn": ["PARTIAL"], "critical": ["FAULT"]},
}


def build_classifier_script():
    """JavaScript classifier body. Thresholds injected as a JSON literal."""
    thresholds_json = json.dumps(THRESHOLDS)
    # NOTE: StreamSets JavaScript Evaluator iterates records as `records`,
    # each record's fields under record.value (a JS object for a MAP record).
    return f"""
var THRESHOLDS = {thresholds_json};

for (var i = 0; i < records.length; i++) {{
  try {{
    var rec = records[i];
    var metric = rec.value['metric'];
    var val = rec.value['value'];
    var severity = 'NORMAL';
    var reason = '';

    var t = THRESHOLDS[metric];
    if (t == null) {{
      // Unknown metric - flag as WARN so it's visible, not silently normal.
      severity = 'WARN';
      reason = 'unknown metric: ' + metric;
    }} else if (t.type === 'categorical') {{
      var v = (val == null) ? '' : ('' + val);
      if (t.critical.indexOf(v) >= 0) {{ severity = 'CRITICAL'; reason = 'valve ' + v; }}
      else if (t.warn.indexOf(v) >= 0) {{ severity = 'WARN'; reason = 'valve ' + v; }}
      else if (t.normal.indexOf(v) >= 0) {{ severity = 'NORMAL'; reason = 'valve ' + v; }}
      else {{ severity = 'WARN'; reason = 'unexpected valve state: ' + v; }}
    }} else {{
      // numeric
      if (val == null) {{
        severity = 'CRITICAL';
        reason = 'null/missing value (data-quality)';
      }} else {{
        var x = parseFloat(val);
        var c = t.critical, w = t.warn;
        if (x < c[0] || x > c[1]) {{
          severity = 'CRITICAL';
          reason = metric + ' ' + x + ' breached critical [' + c[0] + ',' + c[1] + ']';
        }} else if (x < w[0] || x > w[1]) {{
          severity = 'WARN';
          reason = metric + ' ' + x + ' breached warn [' + w[0] + ',' + w[1] + ']';
        }} else {{
          severity = 'NORMAL';
          reason = metric + ' ' + x + ' within normal';
        }}
      }}
    }}

    rec.value['severity'] = severity;
    rec.value['severity_reason'] = reason;
    // threshold_type: SHORT code for the anomaly_events.threshold_type column
    // (varchar(20)) - must stay <=20 chars. Use severity + metric-kind.
    var ttype = severity;  // 'NORMAL'/'WARN'/'CRITICAL' - all <=8 chars
    rec.value['threshold_type'] = ttype;
    // The anomaly_events.value column is numeric. Categorical metrics
    // (valve_status: 'FAULT'/'PARTIAL'/'OPEN') have STRING values that can't be
    // coerced to numeric. Null them so the row still records (severity_reason
    // carries the valve state). Numeric metrics keep their value.
    var vv = rec.value['value'];
    if (vv != null && isNaN(parseFloat(vv))) {{
      rec.value['value'] = null;
    }}
    rec.value['classified_at'] = (new Date()).getTime();
    // Truncate timestamp microseconds (6 digits) to milliseconds (3) so the
    // Field Type Converter's SimpleDateFormat mask can parse it. e.g.
    // 2026-06-24T13:25:15.195902+00:00 -> 2026-06-24T13:25:15.195+00:00
    var rawts = rec.value['timestamp'];
    if (rawts != null) {{
      var sFull = '' + rawts;
      var dot = sFull.indexOf('.');
      if (dot >= 0) {{
        // find where the offset starts after the fractional seconds (+, -, or Z)
        var rest = sFull.substring(dot + 1);
        var offIdx = rest.length;
        for (var k = 0; k < rest.length; k++) {{
          var ch = rest.charAt(k);
          if (ch === '+' || ch === '-' || ch === 'Z') {{ offIdx = k; break; }}
        }}
        var frac = rest.substring(0, offIdx);
        var off = rest.substring(offIdx);
        var frac3 = (frac + '000').substring(0, 3);
        rec.value['timestamp'] = sFull.substring(0, dot + 1) + frac3 + off;
      }}
    }}
    output.write(rec);
  }} catch (e) {{
    error.write(records[i], 'classifier error: ' + e);
  }}
}}
"""


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

    # Fetch the alert_notification_dispatcher fragment (CRITICAL -> maintenance API).
    alert = sch.pipelines.get(name="alert_notification_dispatcher", fragment=True)
    print(f"  alert fragment: {alert.name} v{alert.version}")

    print(f"Building '{PIPELINE_NAME}' (step 4b: + CRITICAL alert dispatch) ...")
    builder = sch.get_pipeline_builder(
        engine_type=eng["engine_type"], engine_id=eng["engine_id"],
    )

    consumer = builder.add_stage("Kafka Multitopic Consumer", type="origin")
    consumer.broker_uri = "kafka:29092"
    consumer.topic_list = ["sensors.normalised"]
    consumer.consumer_group = "oil_gas_anomaly_detection"
    consumer.data_format = "JSON"
    consumer.auto_offset_reset = "EARLIEST"

    classifier = builder.add_stage("JavaScript Evaluator")
    classifier.script = build_classifier_script()

    # Archive tap: EVERY record goes to archive (placeholder Trash for step 3;
    # becomes MongoDB in step 4). The classifier output fans out to both the
    # archive and the severity selector.
    archive = builder.add_stage("Trash")   # -> MongoDB archive in step 4

    # Severity selector: route the non-normal records for anomaly handling.
    selector = builder.add_stage("Stream Selector", type="processor")

    # anomaly_events JDBC Producer (shared by WARN + CRITICAL lanes).
    # Maps classifier fields -> anomaly_events columns. value is numeric(10,2);
    # valve_status (string) records map value to null (severity_reason carries
    # the state). threshold_type carries the severity for now.
    anomaly_db = builder.add_stage("JDBC Producer", type="destination")
    anomaly_db.jdbc_connection_string = "jdbc:postgresql://postgres:5432/oilgas"
    anomaly_db.jdbc_driver_class_name = "org.postgresql.Driver"
    anomaly_db.schema_name = "public"
    anomaly_db.table_name = "anomaly_events"
    anomaly_db.use_credentials = True
    anomaly_db.username = "kermit"
    anomaly_db.password = "kermit"
    anomaly_db.field_to_column_mapping = [
        {"field": "/well_id",        "columnName": "well_id"},
        {"field": "/field_name",     "columnName": "field_name"},
        {"field": "/timestamp",      "columnName": "event_ts"},
        {"field": "/metric",         "columnName": "metric"},
        {"field": "/value",          "columnName": "value"},
        {"field": "/severity",       "columnName": "severity"},
        {"field": "/threshold_type", "columnName": "threshold_type"},
    ]

    # Field Type Converter: convert /timestamp (ISO string with offset +
    # microseconds) to ZONED_DATETIME using ISO_ZONED_DATE_TIME, which parses the
    # offset and variable fractional seconds natively (SimpleDateFormat can't do
    # 6-digit microseconds; ISO_ZONED_DATE_TIME can). Maps to event_ts (timestamptz).
    ts_converter = builder.add_stage("Field Type Converter")
    ts_converter.conversion_method = "BY_FIELD"
    ts_converter.conversions_by_field_name = [{
        "fields": ["/timestamp"],
        "targetType": "DATETIME",
        "treatInputFieldAsDate": False,
        "dataLocale": "en,US",
        "scale": -1,
        "decimalScaleRoundingStrategy": "ROUND_UNNECESSARY",
        "dateFormat": "OTHER",
        "otherDateFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "zonedDateTimeFormat": "ISO_ZONED_DATE_TIME",
        "encoding": "UTF-8",
        "inputFieldEmpty": "ERROR",
    }]

    normal_out = builder.add_stage("Trash")    # normal needs no anomaly action

    # Wire: classifier fans out to archive AND selector
    consumer >> classifier
    classifier >> archive
    classifier >> selector

    # Stream Selector with 2 lanes (StreamSets forbids two lanes feeding the SAME
    # downstream stage - VALIDATION_0039 - so we use ONE lane for all anomalies):
    #   lane[0]: severity != NORMAL  (WARN or CRITICAL) -> anomaly_events
    #   lane[1]: default (NORMAL)                       -> Trash
    anomaly_lane = selector.instance_name + "OutputLaneAnomaly"
    default_lane = selector.output_lanes[0]
    selector.condition = [
        {"outputLane": anomaly_lane, "predicate": "${record:value('/severity') != 'NORMAL'}"},
        {"outputLane": default_lane, "predicate": "default"},
    ]
    # Second selector: from the anomaly flow, peel off CRITICAL for alerting.
    crit_selector = builder.add_stage("Stream Selector", type="processor")
    alert_frag = builder.add_fragment(alert)
    alert_trash = builder.add_stage("Trash")   # non-critical anomalies (WARN) - no alert

    # Wire: lane[0] anomalies -> ts_converter ; lane[1] normal -> Trash.
    # ts_converter (anomaly-only flow) fans its single output to BOTH the JDBC
    # write AND the critical selector. A processor's single output stream CAN
    # fan out to two stages (both receive the SAME records) - unlike a Stream
    # Selector whose multiple lanes are consumed sequentially by each '>>'.
    selector >> ts_converter   # lane 0 (WARN+CRITICAL) -> convert timestamp
    selector >> normal_out     # lane 1 (NORMAL)        -> Trash
    ts_converter >> anomaly_db      # converted anomalies -> anomaly_events (Postgres)
    ts_converter >> crit_selector   # converted anomalies ALSO -> critical selector (fan-out)

    # crit_selector: lane[0] CRITICAL -> alert dispatcher ; lane[1] default(WARN) -> Trash
    crit_lane = crit_selector.instance_name + "OutputLaneCritical"
    crit_default = crit_selector.output_lanes[0]
    crit_selector.condition = [
        {"outputLane": crit_lane, "predicate": "${record:value('/severity') == 'CRITICAL'}"},
        {"outputLane": crit_default, "predicate": "default"},
    ]
    crit_selector >> alert_frag    # lane 0 (CRITICAL) -> alert_notification_dispatcher -> maintenance API
    crit_selector >> alert_trash   # lane 1 (WARN)     -> Trash (recorded in anomaly_events, no alert)

    pipeline = builder.build(
        title=PIPELINE_NAME,
        description=(
            "Oil & Gas anomaly detection (step 2). Kafka sensors.normalised -> "
            "JavaScript severity classifier (thresholds.yaml) -> Trash. Routing + "
            "destinations next. KSEA."
        ),
        labels=pub["labels"],
    )

    if args.dry_run:
        print("DRY RUN - not published.")
        print("  flow: Kafka >> JavaScript classifier >> Trash")
        print("  sets /severity, /severity_reason, /classified_at")
        return

    sch.publish_pipeline(
        pipeline,
        commit_message=f"{PIPELINE_NAME} - step4b + CRITICAL alert dispatch (KSEA)",
    )
    print(f"Published '{PIPELINE_NAME}'.")
    print("Recreate the job. Test: simulator --force-critical 7 or --scenario critical-demo")


if __name__ == "__main__":
    main()
