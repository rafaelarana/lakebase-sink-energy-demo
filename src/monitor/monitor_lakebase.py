#!/usr/bin/env python3
"""Lakebase landing monitor — watch rows land in `asset_live_state` and print pipeline latency.

Every interval (default 5s) it reports, straight from Postgres:
  • upserts in the window + implied rate (how fast rows are landing)
  • distinct sensors touched, and total rows in the table
  • sink staleness  = now() - max(updated_ts)        (how fresh the live state is)
  • event->upsert latency p50/p95 = updated_ts - reading_ts over rows updated in the window
    (the end-to-end time from a reading happening to it being visible in Lakebase)

Pairs with src/ingest/simulate_injection.py. Reads connection details from
provisioning/setup.env (PROFILE, LAKEBASE_PROJECT_ID). A fresh short-lived OAuth credential is
minted each tick, so it runs indefinitely without token expiry.

    python monitor_lakebase.py                     # 5s interval, runs until Ctrl-C
    python monitor_lakebase.py --interval 5 --duration 120
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import psycopg
from databricks.sdk import WorkspaceClient

BRANCH, ENDPOINT = "production", "primary"


def load_env(state_file):
    cfg = {}
    if os.path.exists(state_file):
        for line in open(state_file):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                cfg[k] = v
    return cfg


def connect(profile, project, database):
    ws = WorkspaceClient(profile=profile)
    res = f"projects/{project}/branches/{BRANCH}/endpoints/{ENDPOINT}"
    dns = ws.api_client.do("GET", f"/api/2.0/postgres/{res}")["status"]["hosts"]["host"]
    cred = ws.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": res})
    user = ws.current_user.me().user_name
    return psycopg.connect(host=dns, port=5432, dbname=database, user=user,
                           password=cred["token"], sslmode="require", autocommit=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Monitor rows landing in Lakebase asset_live_state")
    ap.add_argument("--interval", type=int, default=5, help="seconds between reports")
    ap.add_argument("--duration", type=int, default=0, help="0 = run until Ctrl-C")
    ap.add_argument("--table", default="public.asset_live_state")
    ap.add_argument("--progress", action="store_true",
                    help="show ACTUAL per-batch sink write volume from public.stream_progress "
                         "(numOutputRows), instead of the asset_live_state row-landing view")
    args = ap.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    cfg = load_env(os.path.join(root, "provisioning", "setup.env"))
    profile = cfg.get("PROFILE") or os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
    project = cfg.get("LAKEBASE_PROJECT_ID")
    database = os.environ.get("LB_DATABASE", "databricks_postgres")
    if not project:
        sys.exit("LAKEBASE_PROJECT_ID not found in provisioning/setup.env — run setup.sh first.")

    iv, tbl = args.interval, args.table

    if args.progress:
        # ACTUAL sink write volume per window, from the listener-fed stream_progress table.
        what = "public.stream_progress (sink write volume)"
        header = (f"  {'elapsed':>7}  {'batches':>7}  {'in_rows':>8}  {'out_rows':>8}"
                  f"  {'writes/s':>8}  {'proc_rps':>8}  {'batch_ms':>8}")
        q = f"""
            SELECT count(*)                              AS batches,
                   coalesce(sum(num_input_rows), 0)      AS in_rows,
                   coalesce(sum(nullif(num_output_rows, -1)), 0) AS out_rows,
                   round(avg(processed_rps))             AS proc_rps,
                   round(avg(batch_duration_ms))         AS batch_ms,
                   bool_or(num_output_rows = -1)         AS out_unknown
            FROM public.stream_progress
            WHERE event_ts > now() - make_interval(secs => {iv})"""

        def fmt(row, elapsed):
            batches, in_rows, out_rows, proc_rps, batch_ms, unknown = row
            # Postgres sum()/round() come back as Decimal — coerce before formatting.
            batches, in_rows, out_rows = int(batches or 0), int(in_rows or 0), int(out_rows or 0)
            proc_rps, batch_ms = float(proc_rps or 0), float(batch_ms or 0)
            outs = f"{out_rows}{'?' if unknown else ''}"
            return (f"  {elapsed:6.1f}s  {batches:7d}  {in_rows:8d}  {outs:>8}"
                    f"  {out_rows / iv:8.0f}  {proc_rps:8.0f}  {batch_ms:8.0f}"), out_rows
    else:
        # Net row-landing view of the upsert target.
        what = tbl
        header = (f"  {'elapsed':>7}  {'upserts/iv':>10}  {'rate/s':>7}  {'sensors':>7}  {'rows':>5}"
                  f"  {'staleness':>10}  {'evt→upsert p50':>14}  {'p95':>7}")
        q = f"""
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE updated_ts > now() - make_interval(secs => {iv}))            AS win_upserts,
                   count(DISTINCT sensor_asset_id) FILTER (WHERE updated_ts > now() - make_interval(secs => {iv})) AS win_sensors,
                   round(EXTRACT(EPOCH FROM (now() - max(updated_ts))) * 1000)                          AS staleness_ms,
                   round((percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_ts - reading_ts)))
                          FILTER (WHERE updated_ts > now() - make_interval(secs => {iv}))) * 1000)       AS p50_ms,
                   round((percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (updated_ts - reading_ts)))
                          FILTER (WHERE updated_ts > now() - make_interval(secs => {iv}))) * 1000)       AS p95_ms
            FROM {tbl}"""

        def fmt(row, elapsed):
            total, ups, sens, stale, p50, p95 = row
            stale_s = f"{stale:.0f}ms" if stale is not None else "—"
            p50s = f"{p50:.0f}ms" if p50 is not None else "—"
            p95s = f"{p95:.0f}ms" if p95 is not None else "—"
            return (f"  {elapsed:6.1f}s  {ups or 0:10d}  {(ups or 0) / iv:7.0f}  {sens or 0:7d}  {total:5d}"
                    f"  {stale_s:>10}  {p50s:>14}  {p95s:>7}"), (ups or 0)

    print(f"→ monitoring {what} on Lakebase project '{project}' (profile {profile}) every {iv}s\n")
    print(header)

    t0 = time.time()
    cumulative = 0
    conn = connect(profile, project, database)   # one persistent connection; re-mint on error
    try:
        while True:
            elapsed = time.time() - t0
            try:
                row = conn.execute(q).fetchone()
            except Exception as e:  # noqa: BLE001 — token/connection drop → reconnect
                print(f"  {elapsed:6.1f}s  (reconnecting: {str(e)[:50]})")
                try:
                    conn = connect(profile, project, database)
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(iv)
                continue
            line, added = fmt(row, elapsed)
            cumulative += added
            print(line)
            if args.duration and elapsed >= args.duration:
                break
            time.sleep(iv)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
    print(f"\nstopped — {cumulative} upserts observed over {time.time() - t0:.0f}s")


if __name__ == "__main__":
    main()
