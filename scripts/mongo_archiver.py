#!/usr/bin/env python3
"""
mongo_archiver.py  - KSEA Oil & Gas secondary event archive
=============================================================
Archives ALL raw sensor events to MongoDB for historical analysis.

Per the brief (Section: MongoDB Event History):
  - Collection: raw_sensor_events
  - Index on well_id and timestamp
  - SECONDARY, NON-BLOCKING write: the main pipeline must not fail if Mongo is
    slow or unavailable. Running the archive as a SEPARATE consumer (not a stage
    inside the pipeline) satisfies this by design - if Mongo is down, only this
    archiver is affected; the ingestion/anomaly pipelines never notice.

Reads from Kafka topic sensors.normalised (the normalised events the ingestion
pipeline produces) using its OWN consumer group, so it does not interfere with
the anomaly_detection pipeline's consumption.

Mongo: oilgas-mongodb:27017, db 'oilgas', user/pass kermit/kermit (authSource admin).
Connects via localhost:27017 from the host (Mongo publishes that port).

Usage:
  python mongo_archiver.py                 # run continuously
  python mongo_archiver.py --from-beginning   # archive existing backlog too
  python mongo_archiver.py --once          # drain currently available, then exit
  python mongo_archiver.py --dry-run       # connect + show what would be written

Deps: pip install kafka-python pymongo --break-system-packages
"""
import argparse
import json
import datetime as dt

MONGO_URI = "mongodb://kermit:kermit@localhost:27017/?authSource=admin"
MONGO_DB = "oilgas"
MONGO_COLLECTION = "raw_sensor_events"

KAFKA_BOOTSTRAP = "localhost:9092"     # host-published Kafka listener
KAFKA_TOPIC = "sensors.normalised"
CONSUMER_GROUP = "oil_gas_mongo_archiver"


def main():
    ap = argparse.ArgumentParser(description="Archive sensor events to MongoDB.")
    ap.add_argument("--from-beginning", action="store_true",
                    help="Also archive existing backlog (earliest offset).")
    ap.add_argument("--once", action="store_true",
                    help="Drain currently available messages, then exit.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Connect and show a sample; do not write.")
    ap.add_argument("--batch", type=int, default=200, help="Mongo insert batch size.")
    args = ap.parse_args()

    from kafka import KafkaConsumer
    from pymongo import MongoClient, ASCENDING

    # --- Mongo setup ---
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")   # fail fast if unreachable
    coll = client[MONGO_DB][MONGO_COLLECTION]
    # Index on well_id + timestamp (brief requirement). Idempotent.
    coll.create_index([("well_id", ASCENDING), ("timestamp", ASCENDING)],
                      name="well_id_timestamp_idx")
    print(f"Mongo connected: {MONGO_DB}.{MONGO_COLLECTION} (index ensured)")
    print(f"  existing document count: {coll.estimated_document_count()}")

    # --- Kafka consumer ---
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=8000 if args.once else float("inf"),
    )
    print(f"Kafka consumer on '{KAFKA_TOPIC}' (group={CONSUMER_GROUP}, "
          f"{'earliest' if args.from_beginning else 'latest'})")

    buf = []
    written = 0
    try:
        for msg in consumer:
            event = dict(msg.value)
            # Add archive metadata
            event["_archived_at"] = dt.datetime.now(dt.timezone.utc).isoformat()

            if args.dry_run:
                print("WOULD WRITE:", json.dumps(event)[:160])
                if written == 0:
                    written += 1
                    continue
                else:
                    break

            buf.append(event)
            if len(buf) >= args.batch:
                coll.insert_many(buf, ordered=False)
                written += len(buf)
                print(f"  archived {written} events...")
                buf = []
    except KeyboardInterrupt:
        print("\nShutdown signal received.")
    finally:
        if buf and not args.dry_run:
            coll.insert_many(buf, ordered=False)
            written += len(buf)
        consumer.close()
        client.close()
        print(f"Done. Archived {written} events to {MONGO_DB}.{MONGO_COLLECTION}.")


if __name__ == "__main__":
    main()
