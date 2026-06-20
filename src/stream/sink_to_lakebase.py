#!/usr/bin/env python3
"""★ The demo's star — Spark Structured Streaming → the Lakebase SINK.

Reads the bronze reading HISTORY (Delta, fed by Zerobus) as a stream, keeps the LATEST
reading per asset, computes OK/HIGH/LOW status against the alarm bands, and **upserts one
row per asset** into a Lakebase Autoscale (Postgres) table via the first-class sink:

    spark.readStream.table(bronze)                      # Delta streaming source (history)
      → newest-per-asset (max_by over a watermark)       # "latest state" semantic
      → join dim_asset (broadcast static) + status       # OK / HIGH / LOW
      → writeStream.format("postgresql")                 # ← the Lakebase sink
          .outputMode("update").option("upsertkey","sensor_asset_id")
      → public.asset_live_state                          # 1 row / asset, served with ms lookups

Requires DBR 18+ on CLASSIC compute (the sink does not support serverless). This is the
runnable companion to the blog: blog/lakebase-streaming-sink.md.

Env (set by the DAB stream job via spark_env_vars):
  DEMO_CATALOG, DEMO_OPS_SCHEMA, LB_PROJECT_ID, LB_DATABASE, LB_DBTABLE, TRIGGER_MODE,
  optional LB_ENDPOINT (else built as "<project>.production.primary"), CHECKPOINT_PATH.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CAT = os.environ.get("DEMO_CATALOG", "lakebase_sink_demo")
OPS = os.environ.get("DEMO_OPS_SCHEMA", "ops")
BRONZE = f"{CAT}.{OPS}.bronze_sensor_reading"
DIM = f"{CAT}.{OPS}.dim_asset"

LB_PROJECT = os.environ.get("LB_PROJECT_ID", "lakebase-sink-demo")
LB_ENDPOINT = os.environ.get("LB_ENDPOINT", f"{LB_PROJECT}.production.primary")
LB_DATABASE = os.environ.get("LB_DATABASE", "databricks-postgres")
LB_DBTABLE = os.environ.get("LB_DBTABLE", "public.asset_live_state")
CHK = os.environ.get(
    "CHECKPOINT_PATH", f"/Volumes/{CAT}/{OPS}/checkpoints/asset_live_state")


def latest_per_asset(spark: SparkSession):
    """Stream the bronze history; keep the newest reading per asset (newest-wins upsert)."""
    raw = (spark.readStream.table(BRONZE)
           .withWatermark("reading_ts", "30 seconds"))
    # max_by(struct, reading_ts) → the latest row's fields per key; update mode emits one
    # changed row per asset per trigger — exactly what the upsert wants.
    return (raw.groupBy("sensor_asset_id")
            .agg(F.max_by(F.struct("reading_ts", "value", "quality_code", "scada_tag"),
                          "reading_ts").alias("r"))
            .select("sensor_asset_id", "r.*"))


def enrich(latest, dim):
    return (latest.join(F.broadcast(dim), "sensor_asset_id", "left")
            .withColumn("status",
                F.when(F.col("alarm_high").isNotNull() & (F.col("value") > F.col("alarm_high")), F.lit("HIGH"))
                 .when(F.col("alarm_low").isNotNull() & (F.col("value") < F.col("alarm_low")), F.lit("LOW"))
                 .otherwise(F.lit("OK")))
            .withColumn("updated_ts", F.current_timestamp())
            .select("sensor_asset_id", "scada_tag", "site_code", "site_name", "energy_source",
                    "measurement_type", "unit_of_measure", "value", "quality_code", "status",
                    "reading_ts", "updated_ts"))


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    mode = os.environ.get("TRIGGER_MODE", "processing").lower()
    dim = spark.table(DIM).select(
        "sensor_asset_id", "site_code", "site_name", "energy_source",
        "measurement_type", "unit_of_measure", "alarm_low", "alarm_high")

    out = enrich(latest_per_asset(spark), dim)

    writer = (out.writeStream
              .format("postgresql")                       # ← the Lakebase sink
              .outputMode("update")
              .option("upsertkey", "sensor_asset_id")     # → INSERT ... ON CONFLICT DO UPDATE
              .option("endpoint", LB_ENDPOINT)
              .option("database", LB_DATABASE)
              .option("dbtable", LB_DBTABLE)
              .option("checkpointLocation", CHK)
              .queryName("asset_live_state"))

    query = (writer.trigger(realTime=True) if mode == "realtime"
             else writer.trigger(processingTime=os.environ.get("PROCESSING_INTERVAL", "5 seconds"))).start()

    print(f"[lb-sink] {BRONZE} → Lakebase {LB_ENDPOINT}/{LB_DATABASE}/{LB_DBTABLE} "
          f"(trigger={mode}, checkpoint={CHK})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
