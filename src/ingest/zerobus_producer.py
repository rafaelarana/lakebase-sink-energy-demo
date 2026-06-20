#!/usr/bin/env python3
"""Zerobus producer — streams synthetic energy-fleet telemetry into the bronze Delta table.

This is the ingest edge of the demo:

    [this producer] --gRPC--> Zerobus --> bronze_sensor_reading (Delta history)
        --> Spark Structured Streaming --> Lakebase sink --> asset_live_state

The sensor universe is generated locally from `_resources/` (deterministic — same ids/tags the
setup seeder writes to dim_asset). Values are synthesized from each sensor's spec, with an
occasional out-of-band spike so the live state shows HIGH/LOW alarms.

Auth: an OAuth2 service-principal (M2M) with USE CATALOG/SCHEMA + SELECT + MODIFY on the bronze
table. Serialization: Protobuf. Env: see src/ingest/.env.example.

    python zerobus_producer.py                      # stream forever (Ctrl-C to stop)
    python zerobus_producer.py --max-batches 30 --batch-size 200 --seed 7
"""
from __future__ import annotations

import argparse
import logging
import os
import pathlib
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import (
    AckCallback,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
)

import sensor_reading_pb2  # compiled from schema/sensor_reading.proto (run_producer.sh)

# Make `_resources` importable from the producer's directory.
for _p in [pathlib.Path.cwd(), *pathlib.Path(__file__).resolve().parents]:
    if (_p / "_resources").is_dir():
        sys.path.insert(0, str(_p))
        break
from _resources import fleet  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("lb-sink-producer")

SENSORS = list(fleet.iter_sensors())   # materialize the fleet once
GOOD_QUALITY = 192


class _Acks(AckCallback):
    """Durability acknowledgements (at-least-once delivery)."""

    def __init__(self) -> None:
        self.n = 0

    def on_ack(self, offset: int) -> None:
        self.n += 1
        if self.n % 500 == 0:
            log.info("durable up to offset %d (%d acked)", offset, self.n)

    def on_error(self, offset: int, message: str) -> None:
        log.error("ack error at offset %d: %s", offset, message)


def _reading(rnd: random.Random, spike_prob: float, bad_prob: float):
    s = rnd.choice(SENSORS)
    now = datetime.now(timezone.utc)
    micros = int(now.timestamp() * 1_000_000)
    value = fleet.synth_value(s, now, spike=(rnd.random() < spike_prob), rnd=rnd)
    quality = GOOD_QUALITY if rnd.random() >= bad_prob else rnd.choice([0, 64, 68])
    return sensor_reading_pb2.SensorReading(
        sensor_asset_id=s["sensor_asset_id"], scada_tag=s["scada_tag"], site_code=s["site_code"],
        reading_ts=micros, value=value, quality_code=quality, ingest_ts=micros,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Lakebase-sink demo — Zerobus telemetry producer")
    ap.add_argument("--max-batches", type=int, default=0, help="0 = run forever")
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    endpoint = os.environ["ZEROBUS_SERVER_ENDPOINT"]
    workspace = os.environ["DATABRICKS_WORKSPACE_URL"]
    table = os.environ.get("ZEROBUS_TABLE_NAME", "lakebase_sink_demo.ops.bronze_sensor_reading")
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    interval = float(os.environ.get("EMIT_INTERVAL_SECONDS", "1.0"))
    spike_prob = float(os.environ.get("SPIKE_PROBABILITY", "0.02"))
    bad_prob = float(os.environ.get("BAD_QUALITY_PROBABILITY", "0.01"))
    rnd = random.Random(args.seed)

    sdk = ZerobusSdk(endpoint, workspace)
    acks = _Acks()
    options = StreamConfigurationOptions(record_type=RecordType.PROTO, ack_callback=acks)
    props = TableProperties(table, sensor_reading_pb2.SensorReading.DESCRIPTOR)

    run = {"on": True}
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: run.__setitem__("on", False))

    stream: Optional[object] = None
    sent = batches = 0
    log.info("fleet = %d sensors → %s every %.1fs, batch=%d (Ctrl-C to stop)",
             len(SENSORS), table, interval, args.batch_size)
    try:
        stream = sdk.create_stream(client_id, client_secret, props, options)
        while run["on"]:
            for _ in range(args.batch_size):
                for attempt in range(3):                      # transparent reconnect
                    try:
                        stream.ingest_record_nowait(_reading(rnd, spike_prob, bad_prob))
                        sent += 1
                        break
                    except Exception as e:                    # noqa: BLE001
                        log.warning("ingest failed (%d/3): %s", attempt + 1, e)
                        if any(k in str(e).lower() for k in ("closed", "connection")):
                            try:
                                stream.close()
                            except Exception:                 # noqa: BLE001
                                pass
                            stream = sdk.create_stream(client_id, client_secret, props, options)
                        time.sleep(2 ** attempt)
            stream.flush()
            batches += 1
            log.info("batch %d sent (%d total, %d acked)", batches, sent, acks.n)
            if args.max_batches and batches >= args.max_batches:
                break
            time.sleep(interval)
    finally:
        if stream is not None:
            try:
                stream.flush()
            finally:
                stream.close()
        log.info("stopped — %d sent, %d acked", sent, acks.n)


if __name__ == "__main__":
    main()
