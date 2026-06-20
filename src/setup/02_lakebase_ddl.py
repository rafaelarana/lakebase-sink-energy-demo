#!/usr/bin/env python3
"""Setup task 2 — create the Lakebase target table (with a PRIMARY KEY) + grants.

The Lakebase streaming sink upserts via Postgres `INSERT ... ON CONFLICT (<upsertkey>)`,
which needs the target table to carry a PRIMARY KEY on the upsert column. Postgres DDL is
not a DAB resource, so this bundle-managed task creates it over a psycopg connection.

Auth follows the proven Lakebase pattern (see Lumen `scripts/run_lakebase_sql.py`): resolve
the project's primary endpoint DNS and mint a short-lived OAuth token via the Postgres-
Autoscale API, then connect as the running identity (Lakebase auto-creates its PG role).

    GET  /api/2.0/postgres/projects/{p}/branches/{b}/endpoints/{e}   -> status.hosts.host
    POST /api/2.0/postgres/credentials  {"endpoint": "<resource-name>"} -> token
"""
from __future__ import annotations

import argparse

import psycopg
from databricks.sdk import WorkspaceClient

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    sensor_asset_id   TEXT PRIMARY KEY,
    scada_tag         TEXT,
    site_code         TEXT,
    site_name         TEXT,
    energy_source     TEXT,
    measurement_type  TEXT,
    unit_of_measure   TEXT,
    value             DOUBLE PRECISION,
    quality_code      INTEGER,
    status            TEXT,
    reading_ts        TIMESTAMPTZ,
    updated_ts        TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_asset_live_state_status ON {table} (status);
CREATE INDEX IF NOT EXISTS idx_asset_live_state_site   ON {table} (site_code);
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-id", required=True)
    ap.add_argument("--branch", default="production")
    ap.add_argument("--endpoint", default="primary")
    ap.add_argument("--database", default="databricks-postgres")
    ap.add_argument("--dbtable", default="public.asset_live_state")
    args = ap.parse_args()

    ws = WorkspaceClient()
    resource = (f"projects/{args.project_id}/branches/{args.branch}"
                f"/endpoints/{args.endpoint}")

    ep = ws.api_client.do("GET", f"/api/2.0/postgres/{resource}")
    dns = ep["status"]["hosts"]["host"]
    cred = ws.api_client.do("POST", "/api/2.0/postgres/credentials",
                            body={"endpoint": resource})
    user = ws.current_user.me().user_name
    print(f">> Lakebase {dns} db={args.database} as {user}")

    with psycopg.connect(host=dns, port=5432, dbname=args.database,
                         user=user, password=cred["token"], sslmode="require",
                         autocommit=True) as conn:
        conn.execute(DDL.format(table=args.dbtable))
    print(f">> created {args.dbtable} (PRIMARY KEY sensor_asset_id) + indexes")


if __name__ == "__main__":
    main()
