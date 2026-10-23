#!/usr/bin/env python3
"""
wellhead_simulator.py
=====================
Simulates 12 offshore wellheads publishing sensor telemetry over MQTT for the
Kermit StreamSets Engineering Accelerator - Oil & Gas use case.

Each wellhead publishes a JSON sensor event every --interval seconds (default 10s).
By default each reading is published per-metric (one message per metric):
  sensors/wellhead/{id}/{metric}     e.g. sensors/wellhead/7/pressure_bar

The ingestion pipeline subscribes to the per-metric wildcard:
  sensors/wellhead/+/+

Optionally (--publish-combined) the simulator ALSO publishes the full event to
  sensors/wellhead/{id}
This is off by default so the pipeline never sees each reading twice.

Anomaly injection
-----------------
Baseline: every reading drifts realistically around a normal setpoint, with a
small random chance of a WARN/CRITICAL excursion (configurable).

Deliberate (for demos / watchdog testing):
  --force-warn 3              well 3 emits WARN-range readings
  --force-critical 7          well 7 emits CRITICAL-range readings
  --blowout 5                 well 5 emits sustained CRITICAL pressure
                              (trips the 3-in-10-min blowout / watchdog rule)
  --null-readings 9           well 9 emits null pressure/temp (data-quality DLQ test)

Thresholds match the use-case brief exactly (Section 2.2).

Run modes (increasing realism)
------------------------------
  --clean        all readings NORMAL - no random anomalies.
                 Use this during INITIAL pipeline debugging so any anomaly you
                 see is one you deliberately injected, never a random surprise.
  (default)      light random excursions - gentle background realism.
  --realistic    heavier random excursions - stress-test detection / robustness.

Scenario presets (one tidy command for demos)
----------------------------------------------
  --scenario clean           same as --clean
  --scenario realistic       same as --realistic
  --scenario critical-demo   calm field + well 7 CRITICAL (shows case creation)
  --scenario blowout-demo    calm field + well 5 sustained-CRITICAL (trips watchdog)
  --scenario dlq-demo        calm field + well 9 nulls (proves data-quality DLQ)

Usage
-----
  python wellhead_simulator.py --clean                 # initial debugging
  python wellhead_simulator.py                         # light realism
  python wellhead_simulator.py --realistic             # robustness test
  python wellhead_simulator.py --scenario blowout-demo # sign-off demo
  python wellhead_simulator.py --force-critical 7      # manual single fault
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.stderr.write(
        "paho-mqtt is required. Install with:\n"
        "    pip install paho-mqtt\n"
    )
    raise

# --------------------------------------------------------------------------- #
# Thresholds (from brief Section 2.2). normal = (low, high) operating band.
# warn / critical bands are evaluated symmetrically where applicable.
# --------------------------------------------------------------------------- #
THRESHOLDS = {
    "pressure_bar":  {"normal": (150, 280), "warn": (140, 290), "critical": (120, 310)},
    "temp_celsius":  {"normal": (60, 90),   "warn": (55, 95),   "critical": (45, 105)},
    "flow_rate_m3h": {"normal": (800, 1200),"warn": (750, 1250),"critical": (600, 1400)},
    "vibration_g":   {"normal": (0, 2.5),   "warn": (0, 3.0),   "critical": (0, 4.5)},
    # valve_status is categorical: OPEN/CLOSED normal, PARTIAL warn, FAULT critical
}

VALVE_NORMAL = ["OPEN", "CLOSED"]

# Per-metric setpoints the baseline noise oscillates around (centre of normal band)
SETPOINTS = {
    "pressure_bar":  215.0,
    "temp_celsius":  75.0,
    "flow_rate_m3h": 1000.0,
    "vibration_g":   1.0,
}

# Realistic per-metric noise (std dev of small random walk)
NOISE = {
    "pressure_bar":  8.0,
    "temp_celsius":  3.0,
    "flow_rate_m3h": 40.0,
    "vibration_g":   0.4,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulator")


@dataclass
class Wellhead:
    """Holds the drifting state of a single well so readings are continuous."""
    well_id: int
    field_name: str
    state: dict = field(default_factory=dict)

    def __post_init__(self):
        # seed each metric at its setpoint with a tiny per-well offset
        offset = (self.well_id - 6) * 0.5
        for metric, sp in SETPOINTS.items():
            self.state[metric] = sp + offset

    def _drift(self, metric: str) -> float:
        """Random-walk a metric back toward its setpoint (mean reversion)."""
        sp = SETPOINTS[metric]
        cur = self.state[metric]
        # pull 10% toward setpoint + gaussian noise
        nxt = cur + (sp - cur) * 0.10 + random.gauss(0, NOISE[metric])
        if metric == "vibration_g":
            nxt = max(0.0, nxt)
        self.state[metric] = nxt
        return nxt


def _in_band(value, low, high):
    return low <= value <= high


def _force_value(metric: str, severity: str) -> float:
    """Return a value guaranteed to land in the requested severity band."""
    t = THRESHOLDS[metric]
    if severity == "warn":
        # just outside normal but inside critical (pick the high side for clarity)
        if metric == "vibration_g":
            return round(random.uniform(t["normal"][1] + 0.1, t["warn"][1]), 2)
        return round(random.uniform(t["normal"][1] + 1, t["warn"][1]), 2)
    if severity == "critical":
        # outside the critical threshold entirely
        if metric == "vibration_g":
            return round(random.uniform(t["critical"][1] + 0.1, t["critical"][1] + 2.0), 2)
        return round(random.uniform(t["critical"][1] + 1, t["critical"][1] + 30), 2)
    return SETPOINTS[metric]


def classify(metric: str, value) -> str:
    """Classify a single reading as NORMAL / WARN / CRITICAL (mirrors pipeline logic)."""
    if value is None:
        return "CRITICAL"  # null in a required field is a data-quality CRITICAL
    if metric == "valve_status":
        if value in VALVE_NORMAL:
            return "NORMAL"
        return "WARN" if value == "PARTIAL" else "CRITICAL"  # FAULT
    t = THRESHOLDS[metric]
    if _in_band(value, *t["normal"]):
        return "NORMAL"
    if _in_band(value, *t["warn"]):
        return "WARN"
    return "CRITICAL"


def build_event(well: Wellhead, args) -> dict:
    """Produce one complete sensor event for a well, applying injection rules."""
    wid = well.well_id
    forced_warn = wid in args.force_warn
    forced_crit = wid in args.force_critical
    blowout = wid == args.blowout
    nulls = wid in args.null_readings

    readings = {}
    for metric in ("pressure_bar", "temp_celsius", "flow_rate_m3h", "vibration_g"):
        if nulls and metric in ("pressure_bar", "temp_celsius"):
            readings[metric] = None
        elif blowout and metric == "pressure_bar":
            readings[metric] = _force_value(metric, "critical")
        elif forced_crit:
            readings[metric] = _force_value(metric, "critical")
        elif forced_warn:
            readings[metric] = _force_value(metric, "warn")
        else:
            val = well._drift(metric)
            # low-probability random excursion for realism
            roll = random.random()
            if roll < args.critical_prob:
                val = _force_value(metric, "critical")
            elif roll < args.critical_prob + args.warn_prob:
                val = _force_value(metric, "warn")
            elif args.warn_prob == 0 and args.critical_prob == 0:
                # clean mode: keep natural drift inside the normal band so the
                # baseline is a guaranteed calm field (no stray drift anomalies)
                low, high = THRESHOLDS[metric]["normal"]
                val = min(max(val, low + 0.5), high - 0.5)
            readings[metric] = round(val, 2)

    # valve status - faults are driven by the same probabilities, so --clean
    # (prob 0) yields only OPEN/CLOSED, never PARTIAL/FAULT
    if forced_crit or blowout:
        valve = "FAULT"
    elif forced_warn:
        valve = "PARTIAL"
    else:
        r = random.random()
        if r < args.critical_prob:
            valve = "FAULT"
        elif r < args.critical_prob + args.warn_prob:
            valve = "PARTIAL"
        else:
            valve = random.choice(VALVE_NORMAL)

    event = {
        "well_id": f"WELL-{wid:02d}",
        "field_name": well.field_name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pressure_bar": readings["pressure_bar"],
        "temp_celsius": readings["temp_celsius"],
        "flow_rate_m3h": readings["flow_rate_m3h"],
        "vibration_g": readings["vibration_g"],
        "valve_status": valve,
    }
    return event


def event_severity(event: dict) -> str:
    """Highest severity across all metrics in an event (for console logging)."""
    sev_rank = {"NORMAL": 0, "WARN": 1, "CRITICAL": 2}
    worst = "NORMAL"
    for metric in ("pressure_bar", "temp_celsius", "flow_rate_m3h", "vibration_g", "valve_status"):
        s = classify(metric, event[metric])
        if sev_rank[s] > sev_rank[worst]:
            worst = s
    return worst


def make_client(args) -> mqtt.Client:
    # paho-mqtt 2.x requires an explicit callback API version. VERSION2 is the
    # current, non-deprecated API. The callback signatures below match VERSION2.
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"wellhead-simulator-{random.randint(1000,9999)}",
    )

    def on_connect(c, u, flags, reason_code, properties):
        if reason_code == 0:
            log.info("Connected to MQTT broker at %s:%s", args.host, args.port)
        else:
            log.error("MQTT connect failed (reason=%s)", reason_code)

    def on_disconnect(c, u, flags, reason_code, properties):
        if reason_code != 0:
            log.warning("Unexpected MQTT disconnect (reason=%s) - paho will auto-reconnect", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()
    return client


def publish_event(client, event, args):
    wid = event["well_id"].split("-")[1]
    base_topic = f"sensors/wellhead/{int(wid)}"

    # Per-metric is the shape the ingestion pipeline consumes (sensors/wellhead/+/+):
    # one metric per message maps cleanly to one anomaly_events row.
    for metric in ("pressure_bar", "temp_celsius", "flow_rate_m3h", "vibration_g", "valve_status"):
        metric_payload = json.dumps({
            "well_id": event["well_id"],
            "field_name": event["field_name"],
            "timestamp": event["timestamp"],
            "metric": metric,
            "value": event[metric],
        })
        client.publish(f"{base_topic}/{metric}", metric_payload, qos=args.qos)

    # Optional: also publish the full event to sensors/wellhead/{id}. Off by
    # default so the pipeline never double-counts. Enable with --publish-combined.
    if getattr(args, "publish_combined", False):
        client.publish(base_topic, json.dumps(event), qos=args.qos)


def parse_args():
    p = argparse.ArgumentParser(description="Wellhead MQTT telemetry simulator")
    p.add_argument("--host", default="localhost", help="MQTT broker host")
    p.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    p.add_argument("--qos", type=int, default=1, choices=[0, 1, 2], help="MQTT QoS")
    p.add_argument("--wells", type=int, default=12, help="Number of wellheads (default 12)")
    p.add_argument("--interval", type=float, default=10.0, help="Publish interval seconds (default 10)")
    p.add_argument("--duration", type=float, default=0, help="Run for N seconds then stop (0 = forever)")
    p.add_argument("--publish-combined", action="store_true",
                   help="Also publish the full event to sensors/wellhead/{id} (off by default; "
                        "the pipeline consumes per-metric topics, so leave off to avoid double-counting)")
    # --- run mode -----------------------------------------------------------
    # Three ways to run, in increasing realism:
    #   --clean        all readings NORMAL (debug the pipeline with no surprises)
    #   (default)      light random excursions (gentle realism)
    #   --realistic    heavier random excursions (stress / robustness test)
    # The --warn-prob / --critical-prob flags override whatever a mode sets.
    p.add_argument("--clean", action="store_true",
                   help="Force all readings NORMAL - no random anomalies (use during initial pipeline debugging)")
    p.add_argument("--realistic", action="store_true",
                   help="Heavier random anomaly rate to stress-test detection (robustness phase)")
    p.add_argument("--warn-prob", type=float, default=None, help="Override per-metric WARN excursion probability")
    p.add_argument("--critical-prob", type=float, default=None, help="Override per-metric CRITICAL excursion probability")
    # named presets so demo day is one tidy command, not a pile of flags
    p.add_argument("--scenario", choices=["clean", "realistic", "critical-demo", "blowout-demo", "dlq-demo"],
                   default=None, help="Named preset that configures the run (see README)")
    # deliberate injection (lists of well ids)
    p.add_argument("--force-warn", type=int, nargs="*", default=[], metavar="WELL",
                   help="Well id(s) to force into WARN range")
    p.add_argument("--force-critical", type=int, nargs="*", default=[], metavar="WELL",
                   help="Well id(s) to force into CRITICAL range")
    p.add_argument("--blowout", type=int, default=-1, metavar="WELL",
                   help="Well id emitting sustained CRITICAL pressure (trips watchdog blowout rule)")
    p.add_argument("--null-readings", type=int, nargs="*", default=[], metavar="WELL",
                   help="Well id(s) emitting null pressure/temp (DLQ / data-quality test)")
    return p.parse_args()


# Baseline random-excursion rates per run mode (per metric, per cycle)
MODE_RATES = {
    "clean":     {"warn": 0.0,  "critical": 0.0},
    "default":   {"warn": 0.02, "critical": 0.005},   # gentle realism
    "realistic": {"warn": 0.06, "critical": 0.02},    # heavier, stress detection
}


def resolve_mode(args):
    """Translate --scenario / --clean / --realistic into concrete settings.

    Precedence: a named --scenario sets everything, then explicit flags
    (--clean / --realistic / --warn-prob / --critical-prob / --force-*) override.
    """
    # 1) start from a named scenario if given
    if args.scenario == "clean":
        args.clean = True
    elif args.scenario == "realistic":
        args.realistic = True
    elif args.scenario == "critical-demo":
        # calm field, one well clearly CRITICAL - shows the CRITICAL path + case creation
        if not args.force_critical:
            args.force_critical = [7]
    elif args.scenario == "blowout-demo":
        # calm field, one well sustained-CRITICAL pressure - trips the watchdog
        if args.blowout < 0:
            args.blowout = 5
    elif args.scenario == "dlq-demo":
        # calm field, one well emitting nulls - proves data_quality_tagger + DLQ
        if not args.null_readings:
            args.null_readings = [9]

    # demo scenarios want a calm baseline so the injected fault is unambiguous
    if args.scenario in ("critical-demo", "blowout-demo", "dlq-demo") \
            and not args.realistic and args.warn_prob is None and args.critical_prob is None:
        args.clean = True

    # 2) pick the base rate from the mode
    if args.clean:
        rate = MODE_RATES["clean"]
    elif args.realistic:
        rate = MODE_RATES["realistic"]
    else:
        rate = MODE_RATES["default"]

    # 3) explicit prob flags override the mode's rate
    args.warn_prob = rate["warn"] if args.warn_prob is None else args.warn_prob
    args.critical_prob = rate["critical"] if args.critical_prob is None else args.critical_prob
    return args


def main():
    args = parse_args()
    args = resolve_mode(args)
    mode = "CLEAN (all normal)" if args.clean else ("REALISTIC (heavy random)" if args.realistic else "DEFAULT (light random)")
    log.info("Run mode: %s | warn_prob=%.3f critical_prob=%.3f",
             mode, args.warn_prob, args.critical_prob)
    if args.scenario:
        log.info("Scenario preset: %s", args.scenario)
    fields = ["Field-North", "Field-South"]
    wells = [
        Wellhead(well_id=i, field_name=fields[(i - 1) // 6 % 2])
        for i in range(1, args.wells + 1)
    ]

    if args.force_warn:
        log.info("Forcing WARN on wells: %s", args.force_warn)
    if args.force_critical:
        log.info("Forcing CRITICAL on wells: %s", args.force_critical)
    if args.blowout > 0:
        log.info("BLOWOUT mode on well %s (sustained CRITICAL pressure)", args.blowout)
    if args.null_readings:
        log.info("Null readings on wells: %s (data-quality DLQ test)", args.null_readings)

    client = make_client(args)

    stop = {"flag": False}
    def handle_sig(signum, frame):
        log.info("Shutdown signal received, stopping...")
        stop["flag"] = True
    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    start = time.time()
    cycle = 0
    counts = {"NORMAL": 0, "WARN": 0, "CRITICAL": 0}
    log.info("Publishing telemetry for %d wells every %.0fs (Ctrl+C to stop)",
             args.wells, args.interval)
    try:
        while not stop["flag"]:
            cycle += 1
            for well in wells:
                event = build_event(well, args)
                sev = event_severity(event)
                counts[sev] += 1
                publish_event(client, event, args)
                if sev != "NORMAL":
                    log.warning("%-9s %s  P=%s T=%s F=%s V=%s valve=%s",
                                sev, event["well_id"],
                                event["pressure_bar"], event["temp_celsius"],
                                event["flow_rate_m3h"], event["vibration_g"],
                                event["valve_status"])
            log.info("Cycle %d done | events: %d | totals N=%d W=%d C=%d",
                     cycle, args.wells, counts["NORMAL"], counts["WARN"], counts["CRITICAL"])

            if args.duration and (time.time() - start) >= args.duration:
                log.info("Duration %.0fs reached, stopping.", args.duration)
                break

            # sleep in small slices so Ctrl+C is responsive
            slept = 0.0
            while slept < args.interval and not stop["flag"]:
                time.sleep(min(0.5, args.interval - slept))
                slept += 0.5
    finally:
        client.loop_stop()
        client.disconnect()
        log.info("Disconnected. Final totals: N=%d W=%d C=%d",
                 counts["NORMAL"], counts["WARN"], counts["CRITICAL"])


if __name__ == "__main__":
    main()
