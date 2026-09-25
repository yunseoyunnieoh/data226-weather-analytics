# Weather Prediction Analytics -- Seoul & Toronto

**SJSU DATA 226 team lab.** Ingests daily weather for Seoul and Toronto from
the Open-Meteo API into Snowflake, transforms it with dbt into per-city
analytics (moving averages, temperature anomaly, rolling precipitation, dry
spell length), and visualizes the result in a BI dashboard. One Airflow
pipeline and one dbt project handle both cities -- city is configuration,
not a fork in the code.

## Overview

| | |
| --- | --- |
| Data source | [Open-Meteo Forecast API](https://open-meteo.com/en/docs) (`/v1/forecast`) |
| Cities | Seoul, South Korea &nbsp;&middot;&nbsp; Toronto, Canada |
| Orchestration | Apache Airflow 2.10.1 (Docker, `LocalExecutor`) |
| Warehouse | Snowflake |
| Transformation | dbt (`dbt-core` 1.8.7, `dbt-snowflake` 1.8.1) |
| BI | [Preset](https://preset.io) (cloud-hosted Superset) (see [Dashboard](#dashboard)) |

## Architecture

Full diagram and narrative: [`docs/architecture.md`](docs/architecture.md).

```
Open-Meteo API --> Airflow ETL DAG --> Snowflake RAW
    --> [triggers] --> Airflow dbt DAG --> dbt staging/intermediate/analytics
    --> Snowflake analytics layer --> BI dashboard
```

## Repository structure

```
.
├── README.md
├── requirements.txt
├── docker-compose.yaml       # local Airflow, adapted from the course compose file
├── .env.example              # copy to .env; holds no real secrets
├── dags/
│   ├── weather_etl_dag.py    # extract -> transform -> load, then triggers the dbt DAG
│   └── weather_dbt_dag.py    # dbt run -> dbt snapshot -> dbt test
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example  # copy to profiles.yml (gitignored); never commit real one
│   ├── models/
│   │   ├── staging/          # stg_weather.sql + source.yml
│   │   ├── intermediate/     # int_weather_metrics.sql
│   │   ├── analytics/        # weather_analytics.sql
│   │   └── schema.yml        # tests
│   └── snapshots/
│       └── weather_snapshot.sql
├── sql/
│   └── create_tables.sql     # raw table DDL (also embedded in the DAG)
├── docs/
│   ├── architecture.md       # Mermaid diagram
│   └── report.md             # full lab report draft
└── screenshots/
    └── README.md             # checklist of screenshots that need manual capture
```

## Cities and configuration

Both cities are defined in a single Airflow Variable, `CITY_CONFIG`:

```json
{
  "Seoul":   {"latitude": 37.5665, "longitude": 126.9780, "timezone": "Asia/Seoul"},
  "Toronto": {"latitude": 43.6532, "longitude": -79.3832, "timezone": "America/Toronto"}
}
```

`weather_etl_dag.py` loops over this JSON, so adding a third city requires
adding one entry here -- no DAG or dbt code changes.

## Pipeline

1. **`weather_etl_dag`** (`dags/weather_etl_dag.py`, TaskFlow API):
   - `extract`: one Open-Meteo request per city (`past_days=60,
     forecast_days=1`).
   - `transform`: flattens each city's response into rows, validates array
     lengths, rejects empty batches.
   - `load`: upserts rows into `DEV.RAW.CITY_WEATHER_DAILY` via a `MERGE`
     keyed on `(city, date)`, inside an explicit `BEGIN`/`COMMIT`
     transaction with a duplicate-key check before commit and a `ROLLBACK`
     + re-raise on any failure. See [Idempotency](#idempotency).
   - Final task `trigger_dbt_dag` starts `weather_dbt_dag`.
2. **`weather_dbt_dag`** (`dags/weather_dbt_dag.py`, `schedule=None`, only
   ever triggered by DAG 1): `dbt run -> dbt snapshot -> dbt test` via
   `BashOperator`, against the dbt project mounted at `/opt/airflow/dbt`.

## Idempotency

Snowflake does not enforce declared `PRIMARY KEY` constraints on standard
tables, so idempotency comes from application logic, not the DDL:

- Rows are staged, then `MERGE`d on `(city, date)` -- a rerun for a date
  that already exists **updates** that row instead of inserting a second
  copy.
- A `SELECT ... GROUP BY city, date HAVING COUNT(*) > 1` check runs before
  `COMMIT` as an explicit verification, not an assumption.
- Any exception triggers `ROLLBACK` and is re-raised (never swallowed), so
  Airflow correctly marks the task as failed.

## dbt

`dbt/README.md` has the full breakdown. Summary: `stg_weather` (view) ->
`int_weather_metrics` (ephemeral) -> `weather_analytics` (table), all window
functions `PARTITION BY city`. Metrics: daily average temperature, 7-day
moving average temperature, temperature anomaly, 7-day rolling
precipitation, dry spell length. One snapshot (`weather_snapshot`, `check`
strategy) and schema tests (`unique`/`not_null` on the city+date key,
`not_null` on city/date, `accepted_values` on city).

## Snowflake tables

See `docs/report.md` Section 8 for full column-level detail. Short version:

- `DEV.RAW.CITY_WEATHER_DAILY` -- raw landing table, PK `(city, date)`.
- `DEV.ANALYTICS.weather_analytics` -- dbt's final table, source for BI.
- `DEV.SNAPSHOTS.weather_snapshot` -- dbt snapshot of the analytics table.

## Dashboard

**Status: complete.** **Tool:** [Preset](https://preset.io) (cloud-hosted
Apache Superset). **Team/Workspace:** "DATA 226 Weather Analytics" /
"Weather Analytics Lab". **Dataset:** `DEV.ANALYTICS.WEATHER_ANALYTICS`.

**Authentication note (differs from the original plan below):** Preset's
basic connection form expects Snowflake username + password, but this
Snowflake account requires MFA/TOTP, which a plain password-only connection
can't satisfy. The working connection instead uses the same **key-pair
authentication** Airflow and dbt already use, configured through Preset's
**Advanced -> Security -> Secure Extra** field (a JSON blob normally used
for SQLAlchemy `connect_args`) rather than the username/password fields, so
no password or MFA prompt is needed on Preset's side. The encrypted private
key and its passphrase were entered directly into that Secure Extra field in
the browser and were never written to any file in this repository -- see
[Security / secrets](#security--secrets).

**Dashboard "Weather Analytics Dashboard" contains four charts, all reading
`WEATHER_ANALYTICS` and grouped by `city` so Seoul and Toronto are always
shown as separate series:**

1. **Seoul vs. Toronto Daily Avg Temp** -- line chart, X = `WEATHER_DATE`,
   metric = `AVG(DAILY_AVG_TEMP)`, dimension = `CITY`.
2. **7-Day Moving Average Temperature** -- line chart, X = `WEATHER_DATE`,
   metric = `AVG(MOVING_AVG_TEMP_7D)`, dimension = `CITY`.
3. **Temperature Anomaly** -- bar chart, X = `WEATHER_DATE`, metric =
   `AVG(TEMP_ANOMALY)`, dimension = `CITY`.
4. **7-Day Rolling Precipitation** -- bar chart, X = `WEATHER_DATE`, metric =
   `AVG(ROLLING_PRECIP_7D)`, dimension = `CITY`.

Native dashboard filters are configured for **CITY** and for
**WEATHER_DATE** (time range), so a viewer can isolate one city or narrow
the date window and see all four charts update together.

Screenshots `09_dashboard_overview.png` (default filters) and
`10_dashboard_filtered.png` (after changing a filter) are the two required
BI screenshots -- see `screenshots/README.md` for their current status in
this repo.

<details>
<summary>Steps to reproduce this dashboard from scratch (for a teammate or grader setting up their own Preset workspace)</summary>

1. Go to <https://preset.io> and sign up for a free workspace (email or
   Google/GitHub SSO).
2. In the workspace: **Databases** -> **+ Database** -> select **Snowflake**.
   If your account requires MFA, leave the username/password fields as a
   fallback attempt but expect it to fail; instead build a key-pair
   SQLAlchemy URI and put the key/passphrase in **Advanced -> Security ->
   Secure Extra** (JSON, e.g. `{"connect_args": {"private_key": "...",
   ...}}` per Snowflake's SQLAlchemy connector docs) -- never paste the key
   into a field that gets saved to a file you might commit.
   - Account: your Snowflake account locator
   - Warehouse: `COMPUTE_WH`, Database: `DEV`, Role: your role
   - Test the connection, then Save.
3. **Datasets** -> **+ Dataset** -> schema `ANALYTICS`, table
   `WEATHER_ANALYTICS`.
4. Create the four charts listed above (Charts -> + Chart, dataset =
   `WEATHER_ANALYTICS`).
5. **Dashboards** -> **+ Dashboard** -> "Weather Analytics Dashboard", drag
   all four charts in, add a native dashboard filter on `CITY` and a time
   range filter on `WEATHER_DATE`.
6. Save, then capture `09_dashboard_overview.png` at default filters and
   `10_dashboard_filtered.png` after changing a filter.
</details>

## Setup

```bash
git clone <this repo>
cd data226-weather-analytics
cp .env.example .env                    # fill in SNOWFLAKE_KEY_DIR and the passphrase
cp dbt/profiles.yml.example dbt/profiles.yml   # fill in your Snowflake account/user
```

### Required Airflow Variables

| Key | Value |
| --- | --- |
| `CITY_CONFIG` | The JSON shown in [Cities and configuration](#cities-and-configuration) |

### Required Airflow Connections

| Connection Id | Type | Notes |
| --- | --- | --- |
| `snowflake_conn` | Snowflake | Key-pair auth. Login = your Snowflake user, Password = your private key's passphrase, Private Key (Path) = `/opt/airflow/snowflake_keys/rsa_key.p8` (mounted from `SNOWFLAKE_KEY_DIR` in `.env`), Warehouse = `COMPUTE_WH`, Database = `DEV`, Schema = `RAW`. |

### How to run Airflow

```bash
docker compose up -d
docker compose exec airflow airflow dags list-import-errors   # should be empty
```

`docker-compose.yaml` maps the webserver to `${AIRFLOW_HOST_PORT:-8081}`, so
it defaults to `http://localhost:8081` if `AIRFLOW_HOST_PORT` isn't set.
**This project's actual verified run used `http://localhost:8082`**,
because port 8081 was already occupied by the separate HW3 Airflow stack
running on the same machine -- `.env` sets `AIRFLOW_HOST_PORT=8082` to avoid
that conflict. If you don't have anything else on 8081, you can drop that
line from `.env` and use the default. Open whichever port applies (default
login `airflow` / `airflow` unless you changed `_AIRFLOW_WWW_USER_*` in
`.env`), set the Variable and Connection above, unpause `weather_etl_dag`,
and trigger it. `weather_dbt_dag` starts automatically after `load`
succeeds.

### How to run dbt

Either let `weather_dbt_dag` run it inside the container, or run it locally:

```bash
cd dbt
python3 -m venv venv && source venv/bin/activate
pip install dbt-core==1.8.7 dbt-snowflake==1.8.1
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase
dbt debug && dbt run && dbt snapshot && dbt test
```

## Security / secrets

- No password, private key, or account identifier is hardcoded anywhere in
  `dags/` or `dbt/`.
- `dbt/profiles.yml`, `.env`, and any `*.p8`/`*.pub`/`*.pem` key file are
  gitignored (see `.gitignore`). Only `.example` versions with placeholder
  values are committed.
- The Snowflake private key lives on the host filesystem and is mounted
  **read-only** into the Airflow containers; it is never copied into the
  repo or pasted into a committed file.
- Preset's connection to Snowflake uses the same key-pair credentials,
  entered directly into Preset's **Advanced -> Security -> Secure Extra**
  field in the browser (see [Dashboard](#dashboard)). That field lives only
  in Preset's own hosted database connection config, not in this repo or in
  any file on disk here -- it must never be pasted into a README, report,
  commit, or screenshot. The BI screenshots in `screenshots/` show chart/
  dashboard views only, never the database connection setup screen.
- Before pushing, double-check `git status` / `git diff --cached` for any of
  the above filenames.

## Team

[INSERT: team member names, SJSU IDs, and who owned which part -- e.g.
"originally split by city during development; both members reviewed the
unified pipeline before submission."]
