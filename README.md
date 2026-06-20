# Stream straight into Postgres — the Lakebase Structured Streaming sink

The runnable companion to a 2-part blog series:
**[Part 1 — the Lakebase sink](blog/lakebase-streaming-sink.md)** (the concept) ·
**[Part 2 — this project, end to end](blog/lakebase-streaming-sink-part-2.md)** (the build + war stories).

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

- **Schema** (`ops`) + **`checkpoints` volume** inside an *existing* catalog (the catalog is
  an input, not created by the bundle).
- **Lakebase Autoscale project** (`postgres_projects`) — auto-creates the `production`
  branch, `primary` endpoint (min=max=1 CU), and the `databricks_postgres` database.
- **`setup_demo` job** — runs notebooks `01`/`02`: create the bronze table + seed `dim_asset`,
  then create the Postgres `asset_live_state` table **with a PRIMARY KEY** + grants (`psycopg`).
  These are the objects DAB can't declare directly.
- **`stream_to_lakebase` job** — the ★ streaming job (notebook `03`), on a **classic DBR 18
  cluster** (the sink requires DBR 18+ and does **not** support serverless).

The **Zerobus producer runs off-platform** (`src/ingest/`, local venv) — that's the ingest edge.

## Setup

The setup has four phases: **(1)** provision the Databricks objects with DAB, **(2)** create
the Postgres `asset_live_state` table, **(3)** feed bronze with the Zerobus producer, and
**(4)** run the streaming sink. Scripts automate all of it.

### Everything at once

```bash
./run.sh                              # interactive — prompts for profile + catalog (like setup.sh)
./run.sh --profile <p> --catalog <cat>                  # non-interactive
./run.sh --profile <p> --catalog <cat> --start-stream   # …and also launch the sink
```

`run.sh` chains the three scripts below — it prompts for the profile and catalog if you don't
pass them, then each step reuses what the previous resolved. Or run them individually:

### Prerequisites

- A Databricks workspace with **Lakebase** and **Zerobus** enabled, and **DBR 18+** available.
- The **Databricks CLI ≥ 0.287**, authenticated to a profile (`databricks auth login -p <profile>`).
- An **existing Unity Catalog** to deploy into. The bundle creates the *schema, volume,
  Lakebase project and jobs inside it* — it does **not** create the catalog (creating a UC
  catalog needs DAB's `direct` engine + a metastore storage location, which isn't portable).
- **Workspace-admin** rights if you want `scripts/setup_zerobus.sh` to create the producer's
  service principal and grant it.
- `python3`; `uv` for the off-platform producer.

### 1 + 2. Provision everything — `scripts/setup.sh`

The turnkey orchestrator. **Safe by default** (validate only); pass `--apply` to provision.

```bash
scripts/setup.sh                                          # interactive → pick profile/catalog → validate
scripts/setup.sh --profile <p> --catalog <cat> --apply   # deploy + create the tables
```

What `--apply` does, in order:

1. **Tools** — checks `databricks` + `python3`.
2. **Profile** — uses `--profile` (or an interactive picker); verifies the token (logs in if stale).
3. **Catalog** — confirms `--catalog` exists (offers to create if interactive).
4. **Names** — resolves schema / Lakebase project slug / runtime / target.
5. **`bundle validate`**.
6. **`bundle deploy`** — creates the **schema**, **`checkpoints` volume**, **Lakebase Autoscale
   project** (`postgres_projects` → auto `production` branch + `primary` endpoint + database),
   and both **jobs**. If the Lakebase slug is taken/reserved it **auto-retries with a fresh
   slug** and prints the one it used. The sink job deploys **paused** unless `--start-stream`.
7. **`bundle run setup_demo`** — runs the setup notebooks: create `bronze_sensor_reading`
   (Delta, CDF on) + seed `dim_asset` (184 sensors), then create the Postgres
   `asset_live_state` table **with a PRIMARY KEY** + grants.

> Prefer raw bundle commands? Pass the **same `--var`s to every command** — `validate`,
> `deploy`, and `run` each re-resolve variables, so omitting them reverts to the defaults
> (`lakebase_sink_demo`/`ops`) and the job targets the wrong schema. `setup.sh` handles this for you.
> ```bash
> V=(--var=catalog=<cat> --var=ops_schema=ops --var=lakebase_project_id=lbsink-demo-1 --var=dbr_version=18.2.x-scala2.13)
> databricks bundle deploy -t dev -p <profile> "${V[@]}"
> databricks bundle run setup_demo -t dev -p <profile> "${V[@]}"
> ```

### 3. Feed bronze via Zerobus — `scripts/setup_zerobus.sh`

Automates the producer setup **including the service principal**: creates (or reuses) an M2M
service principal, mints its OAuth secret, grants it `USE CATALOG / USE SCHEMA / SELECT /
MODIFY` on the bronze schema, and writes `src/ingest/.env`. It **reuses the values `setup.sh`
resolved** (`provisioning/setup.env`: profile, catalog, *deployed* schema, project) and
**auto-derives the Zerobus endpoint** (workspace id from the `x-databricks-org-id` header +
region from the Lakebase endpoint DNS) — so it needs **no arguments**:

```bash
scripts/setup_zerobus.sh                 # no args — endpoint derived automatically
scripts/run_producer.sh                  # stream forever  (--max-batches N for a bounded run)
```

> Any value can still be overridden with a flag (e.g. `--zerobus-endpoint` to pin it).

### 4. Run the streaming sink

```bash
scripts/start_sink.sh          # reuses the captured config (provisioning/setup.env); no args
scripts/start_sink.sh --stop   # pause it (cluster stops)
```

This deploys the `stream_to_lakebase` job **unpaused**, so the continuous sink starts on a
**classic DBR-18 cluster** and upserts `asset_live_state` from the bronze stream. (Or pass
`--start-stream` to `setup.sh`/`run.sh` to start it during provisioning.)

Then query the live state (`psql` with a Lakebase OAuth token, DBSQL on the registered catalog,
or the SDK — see [Part 2 → "Run the SQL yourself"](blog/lakebase-streaming-sink-part-2.md)):

```sql
SELECT site_name, sensor_asset_id, measurement_type, value, status, reading_ts
FROM   asset_live_state
WHERE  status <> 'OK'              -- which assets are in alarm right now?
ORDER  BY reading_ts DESC;
```

Re-run it: values and `status` advance **in place** (upsert), they don't pile up — that's the
sink doing `INSERT … ON CONFLICT (sensor_asset_id) DO UPDATE`.

### Teardown

```bash
databricks bundle destroy -t dev -p <profile> "${V[@]}"   # same --var values as deploy
```

### Setup gotchas (learned the hard way)

- **The sink is Public Preview** and needs **DBR 18+ on classic** (not serverless). Availability
  varies by workspace/runtime — if `writeStream.format("postgresql")` raises *"Data source
  postgresql does not support streamed writing,"* the preview isn't enabled on that runtime.
- **DBR 18 = Spark 4.1 = scala 2.13** — use `18.x-scala2.13` runtime strings (the default is
  `18.2.x-scala2.13`); a `…-scala2.12` string won't start.
- **The PG database name is `databricks_postgres` (underscore).** The control plane reports
  `databricks-postgres` (hyphen) but Postgres rejects it. The bundle default is correct.
- **Deleted Lakebase slugs stay reserved**, and `postgres_projects` create isn't idempotent —
  `setup.sh` auto-retries with a fresh slug; for raw commands, pass a new `--project-id`.
- **Dev-mode prefixes the schema** — the jobs reference the resolved name, so they stay correct.

> **Cost note.** The sink runs a **classic, always-on** cluster — deploy it paused (default) and
> only `--start-stream` when you want it; `bundle destroy` when done. Lakebase Autoscale scales
> the endpoint to zero when idle.

## The star file

[`notebooks/03_sink_to_lakebase.py`](notebooks/03_sink_to_lakebase.py) — a Databricks notebook
that reads the bronze Delta table as a stream, keeps the newest reading per asset, enriches with
status, and writes via:

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
notebooks/                  # Databricks notebooks run by the jobs:
                            #   01_bronze_and_dim · 02_lakebase_ddl · 03_sink_to_lakebase (★ the sink)
run.sh                      # ← one command: provision + Zerobus + feed bronze (chains the scripts)
src/ingest/                 # Zerobus producer + proto + fleet model (off-platform script)
scripts/setup.sh            # ← turnkey provisioner (deploy + setup job; slug auto-retry)
scripts/setup_zerobus.sh    # ← automate the producer SP + secret + grants + .env
scripts/run_producer.sh     # compile proto + run the producer in a venv
blog/                       # the 2-part Medium series
docs/diagrams/              # architecture diagrams
```

> ℹ️ The notebook widget **defaults** are placeholders (`lakebase_sink_demo`/`ops`); the DAB
> jobs override them via `base_parameters`, so the defaults only matter when you open a notebook
> standalone.

## Credits

The energy fleet (operator, sites, sensor specs, value synthesis) is **entirely fictional** —
invented for this demo, not real assets. Diagrams use the Databricks brand style. Built to
accompany the Medium post.

— *Rafael Arana*
