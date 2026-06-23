# Lakebase streaming sink — live proof (2026-06-23)

The Structured Streaming sink (`writeStream.format("postgresql").option("upsertkey", …)`) was run
end-to-end and **proven live** on Databricks Field Engineering workspace `enerbricks`.

## What blocked it before, and the fix

The 2026-06-21 run failed on **DBR 18.2** with:

```
UnsupportedOperationException: Data source postgresql does not support streamed writing.
```

Root cause: the Lakebase sink requires **DBR 18.3+** (classic compute, dedicated/standard access mode,
not serverless). On DBR < 18.3 `format("postgresql")` resolves to a **batch-only** data source.

Fix: run on a runtime ≥ 18.3. enerbricks had no 18.3 image but offers **19.x (Beta)**, which satisfies
the requirement. Re-ran with `--dbr 19.x-scala2.13`.

## Run

- Workspace: `fevm-enerbricks.cloud.databricks.com` (AWS, `us-east-1`)
- Runtime: `19.x-scala2.13` (classic, single node `m5d.xlarge`)
- Lakebase project: `lbsink-demo-0623` · endpoint `lbsink-demo-0623.production.primary` · db `databricks_postgres`
- Source: 2000 rows fed into `bronze_sensor_reading` via Zerobus (184-sensor fictional fleet), all acked.
- Stream: continuous job `147719769070845`, run `179630762981752` — task `sink` reached `RUNNING / In run`
  with **no UnsupportedOperationException**; first micro-batch committed ~9 min after start (classic cluster warmup).

## Result — `public.asset_live_state` (Lakebase Postgres)

The 2000 bronze readings collapsed to **exactly 184 rows — one per sensor** (the `upsertkey=sensor_asset_id`
INSERT … ON CONFLICT DO UPDATE), confirming upsert (not append) semantics:

```
rows in public.asset_live_state: 184
status breakdown:   OK : 182    HIGH : 2

sample (latest upserts):
  North Ridge Wind | NRDG-WTG02-PWR-ACT  | 1482.70 | OK   | 2026-06-23 17:54:30Z
  North Ridge Wind | NRDG-WTG02-GEN-BRG-T |  109.53 | HIGH | 2026-06-23 17:54:30Z
  Desert Mesa Solar| DSRT-INV03-AC-PWR    |  160.10 | OK   | 2026-06-23 17:54:30Z
  Stormbank Offshore| STBK-WTG02-ROT-SPD  |   12.52 | OK   | 2026-06-23 17:54:30Z
```

Status (`OK`/`HIGH`/`LOW`) is computed in the stream against each asset's alarm bands before the upsert.

## Notes

- **Public Preview** — availability varies by workspace/runtime; needs DBR 18.3+ classic (not serverless).
- All demo infrastructure was torn down after capture (no Lakebase project / jobs / SP retained → $0).
