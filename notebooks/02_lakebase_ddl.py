# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 2 — Lakebase `asset_live_state` table + grants
# MAGIC The streaming sink upserts via `INSERT … ON CONFLICT (<upsertkey>)`, which needs the
# MAGIC target table to carry a `PRIMARY KEY`. Postgres DDL isn't a DAB resource, so this
# MAGIC notebook creates it over a `psycopg` connection, authenticating with a short-lived
# MAGIC Lakebase OAuth token (resolve endpoint DNS → mint credential → connect as the runner).

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1" "databricks-sdk>=0.89.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("project_id", "lakebase-sink-demo")
dbutils.widgets.text("branch", "production")
dbutils.widgets.text("endpoint", "primary")
# NOTE: the control plane shows 'databricks-postgres' (hyphen), but the actual PG database
# name is 'databricks_postgres' (underscore) — use the underscore to connect.
dbutils.widgets.text("database", "databricks_postgres")
dbutils.widgets.text("dbtable", "public.asset_live_state")

project_id = dbutils.widgets.get("project_id")
branch = dbutils.widgets.get("branch")
endpoint = dbutils.widgets.get("endpoint")
database = dbutils.widgets.get("database")
dbtable = dbutils.widgets.get("dbtable")

# COMMAND ----------

import psycopg
from databricks.sdk import WorkspaceClient

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    sensor_asset_id   TEXT PRIMARY KEY,
    scada_tag         TEXT,
    site_code         TEXT,
    site_name         TEXT,
    energy_source     TEXT,
    measurement_type  TEXT,
    unit_of_measure   TEXT,
    value             DOUBLE PRECISION,
    quality_code      INTEGER,
    status            TEXT,
    reading_ts        TIMESTAMPTZ,
    updated_ts        TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_asset_live_state_status ON {table} (status);
CREATE INDEX IF NOT EXISTS idx_asset_live_state_site   ON {table} (site_code);

-- Per-micro-batch sink throughput, written by the streaming job's StreamingQueryListener
-- (notebook 03). Lets the monitor show ACTUAL write volume (num_output_rows), not just the
-- net row-landing view of asset_live_state.
CREATE TABLE IF NOT EXISTS public.stream_progress (
    batch_id          BIGINT,
    event_ts          TIMESTAMPTZ DEFAULT now(),
    num_input_rows    BIGINT,
    num_output_rows   BIGINT,
    input_rps         DOUBLE PRECISION,
    processed_rps     DOUBLE PRECISION,
    batch_duration_ms DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_stream_progress_ts ON public.stream_progress (event_ts);
"""

ws = WorkspaceClient()
resource = f"projects/{project_id}/branches/{branch}/endpoints/{endpoint}"
ep = ws.api_client.do("GET", f"/api/2.0/postgres/{resource}")
dns = ep["status"]["hosts"]["host"]
cred = ws.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": resource})
user = ws.current_user.me().user_name
print(f">> Lakebase {dns} db={database} as {user}")

with psycopg.connect(host=dns, port=5432, dbname=database, user=user,
                     password=cred["token"], sslmode="require", autocommit=True) as conn:
    conn.execute(DDL.format(table=dbtable))
print(f">> created {dbtable} (PRIMARY KEY sensor_asset_id) + indexes")
