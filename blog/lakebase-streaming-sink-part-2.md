# Stream straight into Postgres, Part 2: a renewable fleet's live state, end to end

*Part 1 was the concept. This is the part where we actually build the thing, run it, and I tell you which rakes I stepped on so you don't have to.*

> 📺 **2-part series.**
> **[Part 1](lakebase-streaming-sink.md)** — the Lakebase Structured Streaming sink: the concept, the moves, why it exists.
> **Part 2 (you're here)** — a complete, clone-and-run project, every object provisioned by a Databricks Asset Bundle.

**Repo:** [`github.com/rafaelarana/lakebase-sink-energy-demo`](https://github.com/rafaelarana/lakebase-sink-energy-demo)

---

In Part 1 I showed you the magic five options — `writeStream.format("postgresql")` with an `upsertkey` — and made a lot of noise about deleting your `foreachBatch`. Fair. But a code snippet in a blog post is a promise, not a project. So let's cash the check: a real, runnable demo you can clone, point at a workspace, and watch fill a Postgres table with live state.

Here's the scenario.

## The scenario: Enerbricks wants to know *now*

**Enerbricks** (entirely fictional, no real assets harmed) runs a small European fleet — onshore and offshore wind, a couple of solar parks, a couple of battery sites. Their sensors never shut up: gearbox temperatures, power output, rotor speed, state-of-charge, irradiance, the works.

Two different people want two different things from that firehose:

- The **operations team** needs the **current** state of every asset — value, OK/HIGH/LOW status, last seen — answerable in single-digit milliseconds, because their live wall-board refreshes every couple of seconds and "the data is 15 minutes old" is not an acceptable sentence in a control room.
- The **analysts** want the **whole history** — every reading, forever — to back-test, audit, and train models.

One firehose, two homes, each on the engine it's actually good at:

| Surface | Where | Shape | For |
|---|---|---|---|
| **History** | `ops.bronze_sensor_reading` (Delta / Unity Catalog) | every reading, append-only | analytics, audit, back-test |
| **Live state** | `asset_live_state` (Lakebase / Postgres) | **one upserted row per asset** | the live ops board, ms point lookups |

That's the whole idea: **append the firehose to Delta, upsert the *latest* into Lakebase.** Part 1's "latest state per device," wearing a hard hat.

![Architecture](./diagrams/02-streaming-sink-architecture.png)
*Zerobus simulates the fleet → bronze Delta history → Spark Structured Streaming keeps the newest reading per asset and upserts it into Lakebase via the sink → an app reads current state with ms lookups. Notice what's missing: any connection-pool code.*

## The build, piece by piece

The repo is laid out so each piece is obvious:

```
databricks.yml            # the DAB bundle — creates EVERY object
resources/objects.yml     # schema · volume · Lakebase Autoscale project
resources/jobs.yml        # setup job + the streaming-sink job (classic DBR 18.3+)
notebooks/                # Databricks notebooks the jobs run:
                          #   01_bronze_and_dim · 02_lakebase_ddl · 03_sink_to_lakebase ★
src/ingest/               # Zerobus producer + fleet model (runs off-platform)
scripts/setup.sh          # one turnkey command
```

### 1. The simulator (Zerobus → bronze Delta)

There's no real SCADA system in a demo, so a small **Zerobus** producer invents the fleet from a deterministic model (`src/ingest/_resources/`) and streams Protobuf records over gRPC straight into the bronze Delta table. Every reading also gets the occasional out-of-band spike, so the live board actually lights up with alarms instead of a wall of green.

It runs **off-platform** (your laptop, an edge box) — that's the ingest edge, exactly where a real plant gateway would sit:

```bash
cp src/ingest/.env.example src/ingest/.env   # Zerobus endpoint + service-principal creds
scripts/run_producer.sh                       # compiles the proto, streams forever
```

> Don't have Zerobus handy? The streaming-sink half doesn't care where bronze comes from — a `rate`-source notebook or any Delta writer works. The sink is the point; the source is interchangeable.

### 2. The star — `notebooks/03_sink_to_lakebase.py`

This is the file the whole series is about. It reads the bronze history as a stream, keeps the **newest reading per asset**, computes status against the alarm bands, and **upserts one row per asset** into Lakebase:

```python
# newest reading per asset (so the upsert reflects the latest, not whatever arrived last)
latest = (spark.readStream.table(BRONZE)
          .withWatermark("reading_ts", "30 seconds")
          .groupBy("sensor_asset_id")
          .agg(F.max_by(F.struct("reading_ts", "value", "quality_code", "scada_tag"),
                        "reading_ts").alias("r"))
          .select("sensor_asset_id", "r.*"))

out = (latest.join(F.broadcast(dim), "sensor_asset_id", "left")     # alarm bands + site context
       .withColumn("status",
           F.when(F.col("value") > F.col("alarm_high"), "HIGH")
            .when(F.col("value") < F.col("alarm_low"),  "LOW")
            .otherwise("OK"))
       .select("sensor_asset_id", "scada_tag", "site_code", "site_name", "energy_source",
               "measurement_type", "unit_of_measure", "value", "quality_code", "status",
               "reading_ts", F.current_timestamp().alias("updated_ts")))

(out.writeStream
    .format("postgresql")                       # ← the Lakebase sink
    .outputMode("update")
    .option("upsertkey", "sensor_asset_id")     # → INSERT … ON CONFLICT DO UPDATE
    .option("endpoint", LB_ENDPOINT).option("database", LB_DATABASE).option("dbtable", LB_DBTABLE)
    .option("checkpointLocation", CHK)
    .trigger(processingTime="5 seconds")        # or .trigger(realTime=True) on Photon
    .start())
```

The `max_by(struct, reading_ts)` + watermark + `update` mode is the "newest wins" trick: each trigger emits exactly one changed row per asset, and the sink upserts it. The `dim_asset` table (alarm bands + site context, seeded once from the fleet model) rides along as a broadcast join so the live row carries everything the ops board needs.

### 3. Everything via DAB

The whole ask of this build was "**create every Databricks object with a Databricks Asset Bundle**," and that's what `databricks.yml` + `resources/` do:

```yaml
# resources/objects.yml
resources:
  schemas:   { ops: { catalog_name: ${var.catalog}, name: ${var.ops_schema} } }
  volumes:   { checkpoints: { catalog_name: ${var.catalog}, schema_name: ${resources.schemas.ops.name}, name: checkpoints, volume_type: MANAGED } }
  postgres_projects:
    lakebase:                                   # Lakebase Autoscale — auto branch/endpoint/db
      project_id: ${var.lakebase_project_id}
      pg_version: 17
```

`postgres_projects` is the good bit: declare the project and DAB conjures the whole Lakebase Autoscale stack — the `production` branch, a `primary` endpoint, the database — no clicking. A `setup_demo` job then creates the bronze table, seeds `dim_asset`, and runs the Postgres DDL for `asset_live_state` (with its `PRIMARY KEY` — the sink needs it). The `stream_to_lakebase` job runs the sink on a **classic DBR 18.3+ cluster** — the sink needs DBR 18.3+ on classic, dedicated/standard compute (not serverless); on older runtimes `format("postgresql")` is batch-only. (More on *why classic* below.)

### 4. One command

Borrowing the turnkey pattern, `scripts/setup.sh` does the whole dance — pick a profile, confirm the catalog, validate, and (with `--apply`) deploy and run setup:

```bash
scripts/setup.sh --profile <p> --catalog <cat> --apply                # deploy + create tables
scripts/setup.sh --profile <p> --catalog <cat> --apply --start-stream # …and launch the sink
```

Then ask Lakebase what's on fire, in milliseconds:

```sql
SELECT site_name, sensor_asset_id, measurement_type, value, status, reading_ts
FROM   asset_live_state
WHERE  status <> 'OK'              -- which assets are in alarm RIGHT NOW?
ORDER  BY reading_ts DESC;
```

Run it twice. The rows don't pile up — `value` and `status` advance **in place**. That's the sink doing `INSERT … ON CONFLICT (sensor_asset_id) DO UPDATE`, exactly as promised in Part 1, except now it's a real table you can point a dashboard at.

## Run the SQL yourself

Lakebase *is* Postgres, so any client works — you just authenticate with a short-lived OAuth
token as the password (no static credentials to manage). Here's the whole dance from your
laptop with `psql`:

```bash
PROFILE=<your-databricks-profile>
EP=projects/<lakebase-project>/branches/production/endpoints/primary

# 1. resolve the endpoint host
HOST=$(databricks api get /api/2.0/postgres/$EP -p "$PROFILE" | jq -r '.status.hosts.host')

# 2. mint a ~1-hour OAuth token — this is your psql password
TOKEN=$(databricks api post /api/2.0/postgres/credentials -p "$PROFILE" \
          --json "{\"endpoint\":\"$EP\"}" | jq -r '.token')

# 3. connect as your own Databricks identity (Lakebase auto-creates the PG role)
USER=$(databricks current-user me -p "$PROFILE" | jq -r '.userName')
PGPASSWORD="$TOKEN" psql "host=$HOST port=5432 dbname=databricks_postgres user=$USER sslmode=require"
```

(`databricks_postgres` is the project's default database — check the project page if yours
differs.) Now run the live-state queries:

```sql
-- one row per asset; re-run and watch value/status advance IN PLACE (the upsert at work)
SELECT site_name, sensor_asset_id, value, status, reading_ts
FROM   asset_live_state
ORDER  BY updated_ts DESC
LIMIT  20;

-- how many assets are in alarm right now?
SELECT count(*) FILTER (WHERE status = 'HIGH') AS high,
       count(*) FILTER (WHERE status = 'LOW')  AS low,
       count(*)                                AS total
FROM   asset_live_state;

-- the millisecond point lookup an app actually makes
SELECT value, status, reading_ts FROM asset_live_state WHERE sensor_asset_id = 'NRDG-WTG01-PWR-ACT';
```

**Prefer SQL inside Databricks?** [Register the Lakebase database as a Unity Catalog
catalog](https://docs.databricks.com/aws/en/oltp/projects/register-uc) and query it from the
SQL editor or a notebook on a serverless warehouse — same rows, fully governed, no token
juggling. (The same OAuth-token pattern, in Python, is in the repo's `notebooks/02_lakebase_ddl.py`.)

## Go clone it

That's the series: Part 1 gave you the sink and the *why*; Part 2 gave you a whole project and the *how* — history in Delta, live state in Lakebase, every object in a bundle, and a one-command setup. It's all here:

**[`github.com/rafaelarana/lakebase-sink-energy-demo`](https://github.com/rafaelarana/lakebase-sink-energy-demo)**

Clone it, point `scripts/setup.sh` at a workspace, start the producer, and watch `asset_live_state` come alive. Then go delete a `foreachBatch` somewhere. For me.

---

**References**

- Part 1: [Stream straight into Postgres — the Lakebase sink](lakebase-streaming-sink.md)
- [Use Lakebase as a sink for Structured Streaming](https://docs.databricks.com/aws/en/structured-streaming/lakebase)
- [Lakebase Autoscale projects](https://docs.databricks.com/aws/en/oltp/projects/about)
- [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/)
- Repo: [`rafaelarana/lakebase-sink-energy-demo`](https://github.com/rafaelarana/lakebase-sink-energy-demo)
