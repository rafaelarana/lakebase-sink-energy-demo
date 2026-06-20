# Databricks notebook source
# MAGIC %md
# MAGIC # ★ Streaming sink — bronze Delta → Lakebase
# MAGIC Reads the bronze reading history as a stream, keeps the **latest reading per asset**,
# MAGIC computes OK/HIGH/LOW status against the alarm bands, and **upserts one row per asset**
# MAGIC into Lakebase via the first-class sink:
# MAGIC
# MAGIC     writeStream.format("postgresql").outputMode("update").option("upsertkey", "sensor_asset_id")
# MAGIC
# MAGIC Requires **DBR 18+ on classic compute** (the sink does not support serverless). Runs as a
# MAGIC continuous job; `awaitTermination()` keeps the query alive.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakebase_sink_demo")
dbutils.widgets.text("ops_schema", "ops")
dbutils.widgets.text("lb_project_id", "lakebase-sink-demo")
dbutils.widgets.text("lb_endpoint", "")        # default built as "<project>.production.primary"
dbutils.widgets.text("lb_database", "databricks_postgres")
dbutils.widgets.text("lb_dbtable", "public.asset_live_state")
dbutils.widgets.dropdown("trigger_mode", "processing", ["processing", "realtime"])

CAT = dbutils.widgets.get("catalog")
OPS = dbutils.widgets.get("ops_schema")
BRONZE = f"{CAT}.{OPS}.bronze_sensor_reading"
DIM = f"{CAT}.{OPS}.dim_asset"
LB_PROJECT = dbutils.widgets.get("lb_project_id")
LB_ENDPOINT = dbutils.widgets.get("lb_endpoint") or f"{LB_PROJECT}.production.primary"
LB_DATABASE = dbutils.widgets.get("lb_database")
LB_DBTABLE = dbutils.widgets.get("lb_dbtable")
MODE = dbutils.widgets.get("trigger_mode").lower()
CHK = f"/Volumes/{CAT}/{OPS}/checkpoints/asset_live_state"
print(f"{BRONZE} → Lakebase {LB_ENDPOINT}/{LB_DATABASE}/{LB_DBTABLE} (trigger={MODE})")

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

query = (writer.trigger(realTime=True) if MODE == "realtime"
         else writer.trigger(processingTime="5 seconds")).start()
query.awaitTermination()
