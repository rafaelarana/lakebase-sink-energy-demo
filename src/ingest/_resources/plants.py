"""A fictional renewable portfolio for the demo.

Everything here is INVENTED — the operator "Aurora Renewables", the site names, and the
(rounded, representative) coordinates are not real assets. It just needs to be plausible and
varied enough to make an interesting live-state demo. `site_code` is a short stable code used
to build asset ids and SCADA tags. `type` ∈ {onshore_wind, offshore_wind, solar_pv, battery}.
"""

OPERATOR = "Aurora Renewables"

PLANTS = [
    {"site_code": "NRDG", "plant_name": "North Ridge Wind",    "type": "onshore_wind",  "country": "Ireland",        "region": "County Mayo (uplands)",  "latitude": 54.0, "longitude": -9.3, "capacity_mw": 120.0, "commissioned_year": 2018, "num_units": 40},
    {"site_code": "GLNF", "plant_name": "Glenfield Wind",      "type": "onshore_wind",  "country": "Spain",          "region": "Aragón plateau",         "latitude": 41.5, "longitude": -1.0, "capacity_mw": 90.0,  "commissioned_year": 2016, "num_units": 30},
    {"site_code": "HVNW", "plant_name": "Havenwater Offshore", "type": "offshore_wind", "country": "Netherlands",    "region": "North Sea (offshore)",   "latitude": 52.6, "longitude": 3.7,  "capacity_mw": 480.0, "commissioned_year": 2021, "num_units": 60},
    {"site_code": "STBK", "plant_name": "Stormbank Offshore",  "type": "offshore_wind", "country": "United Kingdom", "region": "North Sea (offshore)",   "latitude": 53.8, "longitude": 1.6,  "capacity_mw": 600.0, "commissioned_year": 2022, "num_units": 75},
    {"site_code": "SUNV", "plant_name": "Sunvale Solar Park",  "type": "solar_pv",      "country": "Spain",          "region": "Extremadura (plain)",    "latitude": 38.9, "longitude": -6.3, "capacity_mw": 250.0, "commissioned_year": 2020, "num_units": None},
    {"site_code": "DSRT", "plant_name": "Desert Mesa Solar",   "type": "solar_pv",      "country": "Portugal",       "region": "Alentejo (plain)",       "latitude": 38.0, "longitude": -7.9, "capacity_mw": 180.0, "commissioned_year": 2019, "num_units": None},
    {"site_code": "IRON", "plant_name": "Irongate BESS",       "type": "battery",       "country": "United Kingdom", "region": "Midlands (grid node)",   "latitude": 52.5, "longitude": -1.5, "capacity_mw": 100.0, "commissioned_year": 2022, "num_units": None},
    {"site_code": "HRBR", "plant_name": "Harbor Point BESS",   "type": "battery",       "country": "Ireland",        "region": "East coast (grid node)", "latitude": 53.3, "longitude": -6.2, "capacity_mw": 80.0,  "commissioned_year": 2023, "num_units": None},
]
for _p in PLANTS:
    _p.setdefault("operator", OPERATOR)

# type → (energy_source, business_category, unit_kind, unit_prefix)
TYPE_META = {
    "onshore_wind":  ("wind",    "Onshore Wind",    "turbine",   "WTG"),
    "offshore_wind": ("wind",    "Offshore Wind",   "turbine",   "WTG"),
    "solar_pv":      ("solar",   "Solar PV",        "inverter",  "INV"),
    "battery":       ("storage", "Battery Storage", "container", "BMS"),
}


def energy_source(plant) -> str:
    return TYPE_META[plant["type"]][0]


def business_category(plant) -> str:
    return TYPE_META[plant["type"]][1]


def unit_kind(plant) -> str:
    return TYPE_META[plant["type"]][2]


def unit_prefix(plant) -> str:
    return TYPE_META[plant["type"]][3]
