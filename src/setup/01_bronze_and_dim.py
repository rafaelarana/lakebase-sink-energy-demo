#!/usr/bin/env python3
"""Setup task 1 — create the bronze Zerobus landing table + seed the dim_asset reference.

Runs as a serverless Spark job task in the DAB bundle (catalog/schema/volume already exist
as bundle resources). Creates:

  • ops.bronze_sensor_reading  — the Delta HISTORY table Zerobus appends to (CDF on),
    column-for-column the proto in src/ingest/schema/sensor_reading.proto.
  • ops.dim_asset              — a small reference (one row per sensor) derived from the
    reused fleet model: identity + site context + alarm bands. The streaming job broadcasts
    it to compute OK/HIGH/LOW status and enrich the live state.
"""
import os
import sys
from pathlib import Path

# Make the reused fleet model (src/ingest/_resources) importable.
_HERE = Path(__file__).resolve()
for _p in _HERE.parents:
    if (_p / "ingest" / "_resources").is_dir():
        sys.path.insert(0, str(_p / "ingest"))
        break
from _resources import config, fleet                       # noqa: E402
from _resources.plants import (                             # noqa: E402
    PLANTS, business_category, energy_source,
)

from pyspark.sql import SparkSession
from pyspark.sql import Row

CAT = config.CATALOG
OPS = config.OPS_SCHEMA
BRONZE = f"{CAT}.{OPS}.bronze_sensor_reading"
DIM = f"{CAT}.{OPS}.dim_asset"


def ensure_bronze(spark: SparkSession) -> None:
    """Zerobus landing table — matches sensor_reading.proto 1:1. CDF on for downstream."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE} (
            sensor_asset_id STRING,
            scada_tag       STRING,
            site_code       STRING,
            reading_ts      TIMESTAMP,
            value           DOUBLE,
            quality_code    INT,
            ingest_ts       TIMESTAMP
        ) USING DELTA
        CLUSTER BY (site_code, reading_ts)
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """)
    print(f">> bronze ready: {BRONZE}")


def seed_dim_asset(spark: SparkSession) -> None:
    """One row per sensor — identity + site context + alarm bands, from the fleet model."""
    site_meta = {
        p["site_code"]: {
            "site_name": p["plant_name"], "operator": p["operator"],
            "country": p["country"], "energy_source": energy_source(p),
            "business_category": business_category(p),
        }
        for p in PLANTS
    }
    rows = []
    for s in fleet.iter_sensors():
        m = site_meta.get(s["site_code"], {})
        rows.append(Row(
            sensor_asset_id=s["sensor_asset_id"], scada_tag=s["scada_tag"],
            site_code=s["site_code"], site_name=m.get("site_name"),
            operator=m.get("operator"), country=m.get("country"),
            energy_source=m.get("energy_source"), business_category=m.get("business_category"),
            equipment=s["equipment"], unit_id=s["unit_id"],
            measurement_type=s["measurement"], unit_of_measure=s["unit"],
            alarm_low=(float(s["alarm_low"]) if s["alarm_low"] is not None else None),
            alarm_high=(float(s["alarm_high"]) if s["alarm_high"] is not None else None),
        ))
    df = spark.createDataFrame(rows)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM)
    print(f">> dim_asset seeded: {DIM} ({df.count()} sensors)")


def main() -> None:
    spark = SparkSession.builder.getOrCreate()
    ensure_bronze(spark)
    seed_dim_asset(spark)


if __name__ == "__main__":
    main()
