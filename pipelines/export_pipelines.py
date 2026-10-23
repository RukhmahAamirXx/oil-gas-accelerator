#!/usr/bin/env python3
"""
export_pipelines.py — export the two pipelines as JSON for submission.

Connects to Control Hub via the same sch_config.yaml the build scripts use,
finds the latest commit of each pipeline by name, and writes the export to
pipelines/exports/ as one .zip per pipeline (StreamSets export format).

Usage:
  python pipelines/export_pipelines.py
"""
import sys
from pathlib import Path

import yaml
from streamsets.sdk import ControlHub

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SCH_CFG = ROOT / "fragments" / "sch_config.yaml"
OUT_DIR = HERE / "exports"

PIPELINE_NAMES = [
    "oil_gas__mqtt_ingestion",
    "oil_gas__anomaly_detection",
]


def main():
    if not SCH_CFG.exists():
        print(f"ERROR: missing {SCH_CFG}")
        sys.exit(1)
    cfg = yaml.safe_load(SCH_CFG.read_text())["control_hub"]
    sch = ControlHub(
        credential_id=cfg["credential_id"],
        token=cfg["credential_token"],
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    exported = []

    for name in PIPELINE_NAMES:
        # Find the pipeline by name; SDK returns a SeekableList — filter by exact name.
        matches = [p for p in sch.pipelines if getattr(p, "name", "") == name]
        if not matches:
            print(f"  SKIP {name} (not found in Control Hub)")
            continue
        # Multiple commits possible after republishes — pick the newest.
        # sch.pipelines entries have commit_time / version metadata; sort defensively.
        pipeline = sorted(
            matches,
            key=lambda p: getattr(p, "commit_time", 0) or 0,
            reverse=True,
        )[0]

        print(f"  exporting {name}  (commit version {getattr(pipeline, 'version', '?')}) ...")
        data = sch.export_pipelines([pipeline])  # returns bytes (.zip)

        out_path = OUT_DIR / f"{name}.zip"
        out_path.write_bytes(data)
        exported.append(out_path)
        print(f"    -> {out_path}  ({len(data):,} bytes)")

    if exported:
        print(f"\nExported {len(exported)} pipeline(s) to {OUT_DIR}")
    else:
        print("\nNo pipelines exported. Check pipeline names and Control Hub access.")


if __name__ == "__main__":
    main()
