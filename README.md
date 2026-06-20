# Stream straight into Postgres — the Lakebase Structured Streaming sink

A runnable companion to the blog **[“Stream straight into Postgres: the Lakebase sink that
finally retires `foreachBatch`”](blog/lakebase-streaming-sink.md)**.

A **Zerobus** stream simulator feeds an energy fleet's telemetry into a **Delta** history
table; a Spark **Structured Streaming** job reads that Delta table and writes the **latest
state per asset directly into a Lakebase Autoscale (Postgres)** table via the first-class
sink — `writeStream.format("postgresql")` with `upsertkey`. No `foreachBatch`, no JDBC, no
connection pool. **Every Databricks object is created with a Databricks Asset Bundle (DAB).**

![Architecture](docs/diagrams/02-streaming-sink-architecture.png)

## The idea: history in the lakehouse, live state in Lakebase

Energy plants (wind / solar / battery) stream sensor readings. Two surfaces, each on the DB
engine it's good at:

| Surface | Where | Shape | Used for |
|---|---|---|---|
| **History** | `ops.bronze_sensor_reading` (Delta / Unity Catalog) | every reading, append-only | analytics, audit, back-test |
| **Live state** | `asset_live_state` (Lakebase / Postgres) | **one upserted row per asset** (latest value + OK/HIGH/LOW status) | a live ops app, with ms point lookups |

The streaming job keeps the **newest reading per asset**, computes status against alarm
bands, and **upserts** it — so `asset_live_state` always answers “what's happening *right
now*?” in a single-digit-millisecond `WHERE sensor_asset_id = …` lookup, while Delta keeps
the full time-series.

## What gets created (all via DAB)

`databricks.yml` + `resources/` declare everything; `bundle deploy` creates it and
`bundle destroy` removes it — symmetrically.

- **Catalog** `lakebase_sink_demo` + schema **`ops`** + **`checkpoints`** volume.
- **Lakebase Autoscale project** (`postgres_projects`) — auto-creates the `production`
  branch, `primary` endpoint (min=max=1 CU), and the `databricks-postgres` database.
- **`setup_demo` job** — creates the bronze table + seeds `dim_asset` (Spark), then creates
  the Postgres `asset_live_state` table **with a PRIMARY KEY** + grants (`psycopg`). These
  are the objects DAB can't declare directly.
- **`stream_to_lakebase` job** — the ★ streaming job, on a **classic DBR 18 cluster**
  (the sink requires DBR 18+ and does **not** support serverless).

The **Zerobus producer runs off-platform** (`src/ingest/`, local venv) — that's the ingest edge.

## Quickstart

**Prerequisites:** a Databricks workspace with **Lakebase** and **Zerobus** enabled, a CLI
profile (`databricks auth login`), the Databricks CLI ≥ 0.287, and an M2M service principal
for the producer. The streaming sink needs **DBR 18+** available in your workspace.

```bash
# 1. deploy all objects
databricks bundle validate -p <profile>
databricks bundle deploy   -t dev -p <profile>

# 2. one-time: create the tables, seed dim_asset, create the Lakebase PK table + grants
databricks bundle run setup_demo -t dev -p <profile>

# 3. start the stream simulator locally (off-platform) — fills the Delta history
cp src/ingest/.env.example src/ingest/.env   # then edit: Zerobus endpoint, SP creds
scripts/run_producer.sh

# 4. run the streaming sink job (continuous) — upserts asset_live_state
databricks bundle run stream_to_lakebase -t dev -p <profile>
```

Then query the live state (psql / DBSQL on the registered Lakebase catalog, or the SDK):

```sql
-- current state, one row per asset
SELECT site_name, sensor_asset_id, measurement_type, value, status, reading_ts
FROM   asset_live_state
WHERE  status <> 'OK'              -- which assets are in alarm right now?
ORDER  BY reading_ts DESC;
```

Re-run the query: values and `status` advance **in place** (upsert), they don't pile up —
that's the sink doing `INSERT … ON CONFLICT (sensor_asset_id) DO UPDATE`.

### Teardown

```bash
databricks bundle destroy -t dev -p <profile>
```

> **Cost note.** The Lakebase sink mandates a **classic, always-on** cluster (no
> serverless), so the `stream_to_lakebase` job runs a continuous cluster — stop it
> (`pause_status` / cancel the run) and `bundle destroy` when you're done. Lakebase
> Autoscale scales the endpoint to zero when idle.

## The star file

[`src/stream/sink_to_lakebase.py`](src/stream/sink_to_lakebase.py) — reads the bronze Delta
table as a stream, keeps the newest reading per asset, enriches with status, and writes via:

```python
(out.writeStream
    .format("postgresql")                      # ← the Lakebase sink
    .outputMode("update")
    .option("upsertkey", "sensor_asset_id")    # → INSERT ... ON CONFLICT DO UPDATE
    .option("endpoint", LB_ENDPOINT).option("database", LB_DATABASE).option("dbtable", LB_DBTABLE)
    .option("checkpointLocation", CHK)
    .trigger(processingTime="5 seconds")       # or .trigger(realTime=True) on Photon
    .start())
```

## Options

- **Trigger** — `trigger_mode=processing` (micro-batch, default) or `realtime` (sub-second,
  Photon). Set per target in `databricks.yml` (prod defaults to `realtime`).
- **No Zerobus?** Point the producer's `ZEROBUS_TABLE_NAME` at any table, or feed bronze
  with a `rate`-source notebook — the streaming-sink half is unchanged.
- **Cloud** — `node_type_id` defaults to AWS (`m5d.xlarge`); override for Azure/GCP.

## Repo layout

```
databricks.yml              # DAB bundle (variables, includes, targets)
resources/objects.yml       # catalog · schema · volume · Lakebase project
resources/jobs.yml          # setup_demo job · stream_to_lakebase job (classic DBR 18)
src/ingest/                 # Zerobus producer + proto + fleet model (off-platform)
src/setup/                  # 01 bronze + dim_asset (Spark) · 02 Lakebase DDL + grants (psycopg)
src/stream/sink_to_lakebase.py   # ★ Structured Streaming → Lakebase sink
scripts/run_producer.sh     # compile proto + run the producer in a venv
blog/                       # the Medium post
docs/diagrams/              # architecture diagrams
```

## Credits

The energy fleet (operator, sites, sensor specs, value synthesis) is **entirely fictional** —
invented for this demo, not real assets. Diagrams use the Databricks brand style. Built to
accompany the Medium post.

— *Rafael Arana*
