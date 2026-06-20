# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 1 — bronze table + `dim_asset`
# MAGIC Creates the Zerobus landing table (`bronze_sensor_reading`, CDF on) and seeds the
# MAGIC `dim_asset` reference (one row per sensor: identity + site context + alarm bands) from
# MAGIC the fictional **Enerbricks** fleet. Runs on serverless; parameters via widgets.

# COMMAND ----------

dbutils.widgets.text("catalog", "lakebase_sink_demo")
dbutils.widgets.text("ops_schema", "ops")
CAT = dbutils.widgets.get("catalog")
OPS = dbutils.widgets.get("ops_schema")
BRONZE = f"{CAT}.{OPS}.bronze_sensor_reading"
DIM = f"{CAT}.{OPS}.dim_asset"
print(f"catalog={CAT} ops_schema={OPS}")

# COMMAND ----------

# MAGIC %md ## Fleet model (fictional Enerbricks fleet — mirror of `src/_resources`)

# COMMAND ----------

SAMPLE_UNITS = {"onshore_wind": 5, "offshore_wind": 5, "solar_pv": 4, "battery": 4}
# site_code, plant_name, type, country
PLANTS = [
    ("NRDG", "North Ridge Wind",    "onshore_wind",  "Ireland"),
    ("GLNF", "Glenfield Wind",      "onshore_wind",  "Spain"),
    ("HVNW", "Havenwater Offshore", "offshore_wind", "Netherlands"),
    ("STBK", "Stormbank Offshore",  "offshore_wind", "United Kingdom"),
    ("SUNV", "Sunvale Solar Park",  "solar_pv",      "Spain"),
    ("DSRT", "Desert Mesa Solar",   "solar_pv",      "Portugal"),
    ("IRON", "Irongate BESS",       "battery",       "United Kingdom"),
    ("HRBR", "Harbor Point BESS",   "battery",       "Ireland"),
]
# type → (energy_source, business_category, unit_prefix)
TYPE_META = {
    "onshore_wind":  ("wind",    "Onshore Wind",    "WTG"),
    "offshore_wind": ("wind",    "Offshore Wind",   "WTG"),
    "solar_pv":      ("solar",   "Solar PV",        "INV"),
    "battery":       ("storage", "Battery Storage", "BMS"),
}
# equipment → [(key, measurement, unit, alarm_low, alarm_high)] ; signal is always "PV"
EQUIPMENT = {
    "onshore_wind": {
        "Gearbox":   [("GBX-OIL-T", "temperature", "degC", 15, 75)],
        "Generator": [("GEN-BRG-T", "temperature", "degC", 15, 95),
                      ("PWR-ACT",   "power",       "kW",   None, None),
                      ("ROT-SPD",   "speed",       "rpm",  0, 20)],
        "Nacelle":   [("NAC-VIB",   "vibration",   "mm/s", None, 8),
                      ("WIND-SPD",  "wind_speed",  "m/s",  None, 25)],
    },
    "offshore_wind": None,  # same as onshore (set below)
    "solar_pv": {
        "Inverter": [("AC-PWR",   "power",       "kW",   None, None),
                     ("DC-CURR",  "current",     "A",    None, 1400),
                     ("INV-TEMP", "temperature", "degC", None, 75)],
        "ArrayMet": [("POA-IRR",  "irradiance",  "W/m2", None, 1300)],
    },
    "battery": {
        "PCS":           [("PWR", "power", "kW", None, None)],
        "BatterySystem": [("SOC",       "state_of_charge", "%",    10, 95),
                          ("SOH",       "state_of_health", "%",    70, None),
                          ("CELL-TEMP", "temperature",     "degC", None, 45)],
    },
}
EQUIPMENT["offshore_wind"] = EQUIPMENT["onshore_wind"]

# COMMAND ----------

# MAGIC %md ## Bronze landing table (Zerobus sink target, CDF on)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE} (
        sensor_asset_id STRING, scada_tag STRING, site_code STRING,
        reading_ts TIMESTAMP, value DOUBLE, quality_code INT, ingest_ts TIMESTAMP
    ) USING DELTA
    CLUSTER BY (site_code, reading_ts)
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print(f">> bronze ready: {BRONZE}")

# COMMAND ----------

# MAGIC %md ## Seed `dim_asset` (alarm bands + site context for the streaming join)

# COMMAND ----------

from pyspark.sql import Row

rows = []
for site, name, ptype, country in PLANTS:
    src, cat, prefix = TYPE_META[ptype]
    for u in range(1, SAMPLE_UNITS[ptype] + 1):
        unit_id = f"{site}-{prefix}{u:02d}"
        for equipment, specs in EQUIPMENT[ptype].items():
            for key, meas, unit, lo, hi in specs:
                rows.append(Row(
                    sensor_asset_id=f"{unit_id}-{key}",
                    scada_tag=f"{site}_{prefix}{u:02d}_{key}_PV",
                    site_code=site, site_name=name, country=country,
                    energy_source=src, business_category=cat,
                    equipment=equipment, unit_id=unit_id,
                    measurement_type=meas, unit_of_measure=unit,
                    alarm_low=(float(lo) if lo is not None else None),
                    alarm_high=(float(hi) if hi is not None else None),
                ))
df = spark.createDataFrame(rows)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DIM)
print(f">> dim_asset seeded: {DIM} ({df.count()} sensors)")
