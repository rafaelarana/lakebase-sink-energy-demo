"""Shared configuration for the Lakebase streaming-sink demo.

Catalog / schema names and a few knobs, overridable via environment variables so the same
code runs on any workspace. Imported by the setup seeder AND the producer (one source of truth).
"""
import os

# Unity Catalog layout. Catalog is created by the DAB bundle; default `lakebase_sink_demo`.
CATALOG = os.environ.get("DEMO_CATALOG", "lakebase_sink_demo")
OPS_SCHEMA = os.environ.get("DEMO_OPS_SCHEMA", "ops")   # telemetry: bronze history + dim_asset

# Demo sizing — sampled units per site (keeps the synthetic fleet demo-sized).
SAMPLE_UNITS = {
    "onshore_wind":  int(os.environ.get("DEMO_WIND_UNITS", "5")),
    "offshore_wind": int(os.environ.get("DEMO_WIND_UNITS", "5")),
    "solar_pv":      int(os.environ.get("DEMO_SOLAR_UNITS", "4")),
    "battery":       int(os.environ.get("DEMO_BATTERY_UNITS", "4")),
}


def fq(schema: str, table: str) -> str:
    """Fully-qualified table name."""
    return f"{CATALOG}.{schema}.{table}"
