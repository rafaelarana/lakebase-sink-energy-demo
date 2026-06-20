"""Sensor templates per plant type (original, illustrative).

One source of truth for BOTH the dim_asset seed (alarm bands) AND the producer (which uses
base/noise/diurnal to synthesize readings). Grouped by equipment so each group becomes an
Equipment node in the asset id.

Each spec: key (tag fragment) · measurement · unit · signal (SCADA suffix) ·
alarm_low/high (None = no band that side) · base/min/max (nominal + physical clamp) ·
noise (gaussian sigma) · diurnal ("solar" daytime curve | "wind" gust pattern | None).
"""

WIND_EQUIPMENT = {
    "Gearbox": [
        {"key": "GBX-OIL-T", "measurement": "temperature", "unit": "degC", "signal": "PV", "alarm_low": 15, "alarm_high": 75, "base": 55, "min": 10, "max": 90, "noise": 1.2, "diurnal": None},
    ],
    "Generator": [
        {"key": "GEN-BRG-T", "measurement": "temperature", "unit": "degC", "signal": "PV", "alarm_low": 15,   "alarm_high": 95,   "base": 65,   "min": 10, "max": 110,  "noise": 1.5, "diurnal": None},
        {"key": "PWR-ACT",   "measurement": "power",       "unit": "kW",   "signal": "PV", "alarm_low": None, "alarm_high": None, "base": 1800, "min": 0,  "max": 3300, "noise": 120, "diurnal": "wind"},
        {"key": "ROT-SPD",   "measurement": "speed",       "unit": "rpm",  "signal": "PV", "alarm_low": 0,    "alarm_high": 20,   "base": 13,   "min": 0,  "max": 22,   "noise": 0.6, "diurnal": "wind"},
    ],
    "Nacelle": [
        {"key": "NAC-VIB",  "measurement": "vibration",  "unit": "mm/s", "signal": "PV", "alarm_low": None, "alarm_high": 8,  "base": 2.5, "min": 0, "max": 14, "noise": 0.4, "diurnal": None},
        {"key": "WIND-SPD", "measurement": "wind_speed", "unit": "m/s",  "signal": "PV", "alarm_low": None, "alarm_high": 25, "base": 8,   "min": 0, "max": 30, "noise": 1.5, "diurnal": "wind"},
    ],
}

SOLAR_EQUIPMENT = {
    "Inverter": [
        {"key": "AC-PWR",   "measurement": "power",       "unit": "kW",   "signal": "PV", "alarm_low": None, "alarm_high": None, "base": 2000, "min": 0, "max": 3600, "noise": 80, "diurnal": "solar"},
        {"key": "DC-CURR",  "measurement": "current",     "unit": "A",    "signal": "PV", "alarm_low": None, "alarm_high": 1400, "base": 900,  "min": 0, "max": 1500, "noise": 40, "diurnal": "solar"},
        {"key": "INV-TEMP", "measurement": "temperature", "unit": "degC", "signal": "PV", "alarm_low": None, "alarm_high": 75,   "base": 45,   "min": 5, "max": 90,   "noise": 1.5, "diurnal": "solar"},
    ],
    "ArrayMet": [
        {"key": "POA-IRR", "measurement": "irradiance", "unit": "W/m2", "signal": "PV", "alarm_low": None, "alarm_high": 1300, "base": 700, "min": 0, "max": 1300, "noise": 30, "diurnal": "solar"},
    ],
}

BATTERY_EQUIPMENT = {
    "PCS": [
        {"key": "PWR",     "measurement": "power",   "unit": "kW", "signal": "PV", "alarm_low": None, "alarm_high": None, "base": 0,    "min": -2500, "max": 2500, "noise": 200, "diurnal": None},
    ],
    "BatterySystem": [
        {"key": "SOC",       "measurement": "state_of_charge", "unit": "%",    "signal": "PV", "alarm_low": 10,   "alarm_high": 95, "base": 55, "min": 5,  "max": 100, "noise": 0.5, "diurnal": None},
        {"key": "SOH",       "measurement": "state_of_health", "unit": "%",    "signal": "PV", "alarm_low": 70,   "alarm_high": None,"base": 97, "min": 60, "max": 100, "noise": 0.1, "diurnal": None},
        {"key": "CELL-TEMP", "measurement": "temperature",     "unit": "degC", "signal": "PV", "alarm_low": None, "alarm_high": 45, "base": 28, "min": 5,  "max": 60,  "noise": 0.8, "diurnal": None},
    ],
}

EQUIPMENT_BY_TYPE = {
    "onshore_wind":  WIND_EQUIPMENT,
    "offshore_wind": WIND_EQUIPMENT,
    "solar_pv":      SOLAR_EQUIPMENT,
    "battery":       BATTERY_EQUIPMENT,
}


def equipment_for(plant_type: str) -> dict:
    """Equipment→[sensor spec] map for a plant type."""
    return EQUIPMENT_BY_TYPE[plant_type]


def iter_sensor_specs(plant_type: str):
    """Yield (equipment_name, sensor_spec) for every sensor of a plant type."""
    for equipment, sensors in EQUIPMENT_BY_TYPE[plant_type].items():
        for spec in sensors:
            yield equipment, spec
