# Databricks notebook source
# MAGIC %md
# MAGIC # ★ Streaming sink — bronze Delta → Lakebase
# MAGIC Reads the bronze reading history as a stream, keeps the **latest reading per asset**,
# MAGIC computes OK/HIGH/LOW status against the alarm bands, and **upserts one row per asset**
# MAGIC into Lakebase via the first-class sink:
# MAGIC
# MAGIC     writeStream.format("postgresql").outputMode("update").option("upsertkey", "sensor_asset_id")
# MAGIC
# MAGIC Requires **DBR 18.3+ on classic compute** (dedicated/standard access; the sink does not support
# MAGIC serverless). On DBR <18.3 `format("postgresql")` is batch-only ("does not support streamed writing"). Runs as a
# MAGIC continuous job; `awaitTermination()` keeps the query alive.

# COMMAND ----------

# MAGIC %pip install "psycopg[binary]>=3.1" "databricks-sdk>=0.89.0"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "lakebase_sink_demo")
dbutils.widgets.text("ops_schema", "ops")
dbutils.widgets.text("lb_project_id", "lakebase-sink-demo")
dbutils.widgets.text("lb_endpoint", "")        # default built as "<project>.production.primary"
dbutils.widgets.text("lb_database", "databricks_postgres")
dbutils.widgets.text("lb_dbtable", "public.asset_live_state")
dbutils.widgets.dropdown("trigger_mode", "processing", ["processing", "realtime"])
# Low-latency micro-batch cadence. "1 second" ≈ 1-2s end-to-end; "0 seconds" = run as fast as
# possible (back-to-back batches, lowest latency, highest cluster utilization).
dbutils.widgets.text("processing_interval", "1 second")

CAT = dbutils.widgets.get("catalog")
OPS = dbutils.widgets.get("ops_schema")
BRONZE = f"{CAT}.{OPS}.bronze_sensor_reading"
DIM = f"{CAT}.{OPS}.dim_asset"
LB_PROJECT = dbutils.widgets.get("lb_project_id")
LB_ENDPOINT = dbutils.widgets.get("lb_endpoint") or f"{LB_PROJECT}.production.primary"
LB_DATABASE = dbutils.widgets.get("lb_database")
LB_DBTABLE = dbutils.widgets.get("lb_dbtable")
MODE = dbutils.widgets.get("trigger_mode").lower()
PROC_INTERVAL = dbutils.widgets.get("processing_interval").strip()
CHK = f"/Volumes/{CAT}/{OPS}/checkpoints/asset_live_state"
print(f"{BRONZE} → Lakebase {LB_ENDPOINT}/{LB_DATABASE}/{LB_DBTABLE} "
      f"(trigger={MODE}, interval={PROC_INTERVAL})")

# COMMAND ----------

# MAGIC %md ## Build the stream — newest reading per asset, enriched with status

# COMMAND ----------

from pyspark.sql import functions as F

# Newest reading per asset (so the upsert reflects the latest, not whatever arrived last).
latest = (spark.readStream.table(BRONZE)
          .withWatermark("reading_ts", "30 seconds")
          .groupBy("sensor_asset_id")
          .agg(F.max_by(F.struct("reading_ts", "value", "quality_code", "scada_tag"),
                        "reading_ts").alias("r"))
          .select("sensor_asset_id", "r.*"))

dim = spark.table(DIM).select(
    "sensor_asset_id", "site_code", "site_name", "energy_source",
    "measurement_type", "unit_of_measure", "alarm_low", "alarm_high")

out = (latest.join(F.broadcast(dim), "sensor_asset_id", "left")
       .withColumn("status",
           F.when(F.col("alarm_high").isNotNull() & (F.col("value") > F.col("alarm_high")), F.lit("HIGH"))
            .when(F.col("alarm_low").isNotNull() & (F.col("value") < F.col("alarm_low")), F.lit("LOW"))
            .otherwise(F.lit("OK")))
       .withColumn("updated_ts", F.current_timestamp())
       .select("sensor_asset_id", "scada_tag", "site_code", "site_name", "energy_source",
               "measurement_type", "unit_of_measure", "value", "quality_code", "status",
               "reading_ts", "updated_ts"))

# COMMAND ----------

# MAGIC %md ## Write to Lakebase via the sink (upsert)

# COMMAND ----------

writer = (out.writeStream
          .format("postgresql")                       # ← the Lakebase sink
          .outputMode("update")
          .option("upsertkey", "sensor_asset_id")     # → INSERT ... ON CONFLICT DO UPDATE
          .option("endpoint", LB_ENDPOINT)
          .option("database", LB_DATABASE)
          .option("dbtable", LB_DBTABLE)
          .option("checkpointLocation", CHK)
          .queryName("asset_live_state"))

# COMMAND ----------

# MAGIC %md ## Per-batch sink throughput → Lakebase `stream_progress` (feeds the monitor `--progress` view)

# COMMAND ----------

# A StreamingQueryListener records each micro-batch's metrics (rows in, rows written to the sink,
# rates, duration) into public.stream_progress so the monitor can show ACTUAL write volume, not
# just the net row-landing view of asset_live_state. Failures here never affect the query (caught).
import json
import threading
import psycopg
from pyspark.sql.streaming import StreamingQueryListener
from databricks.sdk import WorkspaceClient

_INSERT = ("INSERT INTO public.stream_progress"
           "(batch_id,num_input_rows,num_output_rows,input_rps,processed_rps,batch_duration_ms)"
           " VALUES (%s,%s,%s,%s,%s,%s)")


class _ProgressToLakebase(StreamingQueryListener):
    def __init__(self):
        self._ws = WorkspaceClient()
        self._res = f"projects/{LB_PROJECT}/branches/production/endpoints/primary"
        self._lock = threading.Lock()
        self._connect()

    def _connect(self):
        ep = self._ws.api_client.do("GET", f"/api/2.0/postgres/{self._res}")
        cred = self._ws.api_client.do("POST", "/api/2.0/postgres/credentials", body={"endpoint": self._res})
        self._conn = psycopg.connect(host=ep["status"]["hosts"]["host"], port=5432, dbname=LB_DATABASE,
                                     user=self._ws.current_user.me().user_name,
                                     password=cred["token"], sslmode="require", autocommit=True)

    def onQueryStarted(self, event): pass

    def onQueryTerminated(self, event): pass

    def onQueryIdle(self, event): pass

    def onQueryProgress(self, event):
        try:
            d = json.loads(event.progress.json)
            sink = d.get("sink") or {}
            dur = d.get("durationMs") or {}
            row = (int(d.get("batchId", -1)), int(d.get("numInputRows", 0) or 0),
                   int(sink.get("numOutputRows", -1)), d.get("inputRowsPerSecond"),
                   d.get("processedRowsPerSecond"), dur.get("triggerExecution"))
            with self._lock:
                try:
                    self._conn.execute(_INSERT, row)
                except Exception:                 # token rotated / connection dropped → reconnect
                    self._connect()
                    self._conn.execute(_INSERT, row)
        except Exception as ex:                   # never let logging break the stream
            print("progress-listener error (ignored):", ex)


spark.streams.addListener(_ProgressToLakebase())
print(">> progress listener attached → public.stream_progress")

# Low-latency micro-batch: processingTime drives the cadence (default "1 second"; "0 seconds" =
# as fast as possible). With a Delta source this is the lowest-latency option — Real-Time Mode
# (realTime=...) is NOT usable here: RTM rejects a Delta input stream
# (STREAMING_REAL_TIME_MODE.INPUT_STREAM_NOT_SUPPORTED). The realtime branch is kept only for a
# future Kafka/Kinesis source; on Delta it will fail by design.
query = (writer.trigger(realTime="1 minute") if MODE == "realtime"
         else writer.trigger(processingTime=PROC_INTERVAL)).start()
query.awaitTermination()
