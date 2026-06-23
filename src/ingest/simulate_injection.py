#!/usr/bin/env python3
"""Zerobus injection simulator — drive a chosen load into the bronze table and report
ingestion latency live.

Unlike `zerobus_producer.py` (fixed batch/interval), this asks for a target rate and a
duration, paces sending to hit that rate, and measures **ingestion latency** = the time from
sending a record to its Zerobus durability ack (FIFO-matched per offset). Progress prints once
per second.

    python simulate_injection.py                       # prompts for rate + duration
    python simulate_injection.py --rate 500 --duration 60
    python simulate_injection.py --rate 2000 --duration 120 --seed 7

Pairs with scripts/monitor_lakebase.py, which watches the rows land in Lakebase.
"""
from __future__ import annotations

import argparse
import collections
import logging
import os
import pathlib
import random
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import AckCallback, RecordType, StreamConfigurationOptions, TableProperties

import sensor_reading_pb2  # compiled from schema/sensor_reading.proto by the wrapper

for _p in [pathlib.Path.cwd(), *pathlib.Path(__file__).resolve().parents]:
    if (_p / "_resources").is_dir():
        sys.path.insert(0, str(_p))
        break
from _resources import fleet  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
SENSORS = list(fleet.iter_sensors())
GOOD_QUALITY = 192


def pct(values, q):
    if not values:
        return float("nan")
    s = sorted(values)
    k = min(len(s) - 1, int(round((len(s) - 1) * q)))
    return s[k]


class _Acks(AckCallback):
    """Match each ack (in offset order) to the oldest pending send time → ingest latency (ms)."""

    def __init__(self) -> None:
        self.sent_ts: "collections.deque[float]" = collections.deque()
        self.acked = 0
        self.latencies_ms: list[float] = []   # rolling; trimmed by the caller
        self.errors = 0

    def on_ack(self, offset: int) -> None:
        now = time.time()
        if self.sent_ts:
            self.latencies_ms.append((now - self.sent_ts.popleft()) * 1000.0)
        self.acked += 1

    def on_error(self, offset: int, message: str) -> None:
        self.errors += 1


def _reading(rnd, spike_prob, bad_prob):
    s = rnd.choice(SENSORS)
    now = datetime.now(timezone.utc)
    micros = int(now.timestamp() * 1_000_000)
    value = fleet.synth_value(s, now, spike=(rnd.random() < spike_prob), rnd=rnd)
    quality = GOOD_QUALITY if rnd.random() >= bad_prob else rnd.choice([0, 64, 68])
    return sensor_reading_pb2.SensorReading(
        sensor_asset_id=s["sensor_asset_id"], scada_tag=s["scada_tag"], site_code=s["site_code"],
        reading_ts=micros, value=value, quality_code=quality, ingest_ts=micros,
    )


def _ask_int(prompt, default):
    if not sys.stdin.isatty():
        return default
    raw = input(f"{prompt} [{default}]: ").strip()
    return int(raw) if raw else default


def main() -> None:
    ap = argparse.ArgumentParser(description="Zerobus injection simulator with live ingest latency")
    ap.add_argument("--rate", type=int, help="signals per second")
    ap.add_argument("--duration", type=int, help="seconds to run")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    rate = args.rate if args.rate is not None else _ask_int("Signals per second", 200)
    duration = args.duration if args.duration is not None else _ask_int("Seconds running", 60)
    if rate <= 0 or duration <= 0:
        sys.exit("rate and duration must be > 0")

    endpoint = os.environ["ZEROBUS_SERVER_ENDPOINT"]
    workspace = os.environ["DATABRICKS_WORKSPACE_URL"]
    table = os.environ.get("ZEROBUS_TABLE_NAME", "lakebase_sink_demo.ops.bronze_sensor_reading")
    client_id = os.environ["DATABRICKS_CLIENT_ID"]
    client_secret = os.environ["DATABRICKS_CLIENT_SECRET"]
    spike_prob = float(os.environ.get("SPIKE_PROBABILITY", "0.02"))
    bad_prob = float(os.environ.get("BAD_QUALITY_PROBABILITY", "0.01"))
    rnd = random.Random(args.seed)

    print(f"→ injecting {rate} signals/s for {duration}s into {table}")
    print(f"  ({len(SENSORS)} sensors, via {endpoint})\n")
    print(f"  {'elapsed':>7}  {'sent':>8}  {'acked':>8}  {'inflight':>8}  {'rate/s':>7}  {'ack-lat p50':>11}  {'p95':>7}")

    sdk = ZerobusSdk(endpoint, workspace)
    acks = _Acks()
    options = StreamConfigurationOptions(record_type=RecordType.PROTO, ack_callback=acks)
    props = TableProperties(table, sensor_reading_pb2.SensorReading.DESCRIPTOR)

    stream: Optional[object] = None
    sent = 0
    t0 = time.time()
    try:
        stream = sdk.create_stream(client_id, client_secret, props, options)
        tick = 0
        while time.time() - t0 < duration:
            tick_start = time.time()
            for _ in range(rate):
                acks.sent_ts.append(time.time())
                stream.ingest_record_nowait(_reading(rnd, spike_prob, bad_prob))
                sent += 1
            stream.flush()
            tick += 1
            elapsed = time.time() - t0
            window = acks.latencies_ms[-rate:] if acks.latencies_ms else []
            print(f"  {elapsed:6.1f}s  {sent:8d}  {acks.acked:8d}  {sent - acks.acked:8d}"
                  f"  {sent / elapsed:7.0f}  {pct(window, 0.50):9.0f}ms  {pct(window, 0.95):5.0f}ms")
            sleep_left = 1.0 - (time.time() - tick_start)
            if sleep_left > 0:
                time.sleep(sleep_left)
    except KeyboardInterrupt:
        print("\n(interrupted)")
    finally:
        if stream is not None:
            try:
                stream.flush()
            finally:
                stream.close()
        # brief drain so trailing acks land in the summary
        t_drain = time.time()
        while acks.acked < sent and time.time() - t_drain < 5:
            time.sleep(0.2)
        alll = acks.latencies_ms
        print(f"\nDONE — sent={sent}, acked={acks.acked}, errors={acks.errors}, "
              f"avg rate={sent / max(1e-9, time.time() - t0):.0f}/s")
        if alll:
            print(f"ingest latency over {len(alll)} acks: "
                  f"p50={pct(alll, 0.5):.0f}ms  p95={pct(alll, 0.95):.0f}ms  max={max(alll):.0f}ms")


if __name__ == "__main__":
    main()
