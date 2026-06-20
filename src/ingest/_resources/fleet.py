"""Sensor enumeration + value synthesis — the single source of truth for sensor identity.

Both the dim_asset seed and the Zerobus producer need the SAME sensors with the SAME ids and
SCADA tags, derived deterministically from `plants.py` + `sensor_specs.py` + `config.SAMPLE_UNITS`
(no workspace query needed at the edge).

  iter_sensors()           → one dict per sensor (id, tag, site, equipment + the spec).
  synth_value(spec, now…)  → a realistic reading for the stream.
"""
import math
import random

from . import config
from .plants import PLANTS, unit_prefix
from .sensor_specs import equipment_for


def iter_units():
    """Yield (plant, unit_index, unit_id, prefix) for each sampled unit."""
    for plant in PLANTS:
        prefix = unit_prefix(plant)
        for u in range(1, config.SAMPLE_UNITS[plant["type"]] + 1):
            yield plant, u, f"{plant['site_code']}-{prefix}{u:02d}", prefix


def iter_sensors():
    """Yield one sensor dict per measurement point (stable id/tag scheme)."""
    for plant, u, unit_id, prefix in iter_units():
        site = plant["site_code"]
        for equipment, specs in equipment_for(plant["type"]).items():
            for spec in specs:
                yield {
                    "sensor_asset_id": f"{unit_id}-{spec['key']}",
                    "scada_tag": f"{site}_{prefix}{u:02d}_{spec['key']}_{spec['signal']}",
                    "site_code": site,
                    "unit_id": unit_id,
                    "equipment": equipment,
                    **spec,
                }


def _daylight(now) -> float:
    """0 at night → ~1 near midday (approximate diurnal curve)."""
    h = now.hour + now.minute / 60.0
    return max(0.0, math.sin((h - 6.0) / 12.0 * math.pi)) if 6 <= h <= 18 else 0.0


def synth_value(spec: dict, now, spike: bool = False, rnd: random.Random = random) -> float:
    """A realistic value for `spec` at time `now`. `spike` pushes it out of its alarm band."""
    base, noise = spec["base"], spec["noise"]
    diurnal = spec.get("diurnal")

    if diurnal == "solar":
        f = _daylight(now)
        val = base * f + rnd.gauss(0, noise) * (0.3 + 0.7 * f)
    elif diurnal == "wind":
        gust = 0.7 + 0.5 * (0.5 + 0.5 * math.sin(now.timestamp() / 600.0))
        val = base * gust + rnd.gauss(0, noise)
    else:
        val = base + rnd.gauss(0, noise)

    if spike and spec["alarm_high"] is not None:
        val = spec["alarm_high"] * rnd.uniform(1.03, 1.25)
    elif spike and spec["alarm_low"] is not None:
        val = spec["alarm_low"] * rnd.uniform(0.5, 0.9)
    elif not spike:
        val = min(spec["max"], max(spec["min"], val))   # clamp (spikes intentionally exceed)

    return round(val, 3)
