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
│   └── weather_dbt_dag.py    # dbt run -> dbt test -> dbt snapshot
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml          # reads env_var('DBT_*'); no personal values, safe to commit
│   ├── models/
│   │   ├── transform/        # source.yml, weather_daily.sql, weather_metrics.sql (ephemeral)
│   │   ├── analytics/        # weather_analytics.sql (table)
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
adding one entry here, plus the new city name in the `accepted_values` test
in `dbt/models/schema.yml`.

## Pipeline

1. **`weather_etl_dag`** (`dags/weather_etl_dag.py`, TaskFlow API):
   - `extract`: one Open-Meteo request per city (`past_days=60,
     forecast_days=1`).
   - `transform`: flattens each city's response into
     `(city, latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)` rows.
   - `load`: creates the `RAW` schema and `RAW.CITY_WEATHER_DAILY` if they
     don't exist, then replaces the table's contents with `DELETE` + `INSERT`
     inside a `BEGIN`/`COMMIT` transaction, with `ROLLBACK` + re-raise on any
     failure. See [Idempotency](#idempotency).
   - Final task `trigger_dbt_dag` starts `weather_dbt_dag`.
2. **`weather_dbt_dag`** (`dags/weather_dbt_dag.py`, `schedule=None`, only
   ever triggered by DAG 1): `dbt run -> dbt test -> dbt snapshot` via
   `BashOperator`, against the dbt project mounted at `/opt/airflow/dbt`.
   Snowflake credentials are read from the `snowflake_conn` Airflow
   Connection and passed to dbt as `DBT_*` environment variables, so both
   DAGs share one set of credentials.

Both DAGs use `max_active_runs=1`, so two loads can never run against the
table at the same time.   

## Idempotency

Snowflake does not enforce declared `PRIMARY KEY` constraints on standard
tables, so idempotency comes from application logic, not the DDL:

- `DELETE FROM RAW.CITY_WEATHER_DAILY` followed by an `INSERT` of every
  extracted row means each run leaves the table holding exactly the current
  61-day window (past 60 days plus today) for each city. 
  Running the DAG twice in a row gives the same result:
  2 cities x 61 days = 122 rows both times.
- `CREATE SCHEMA` / `CREATE TABLE IF NOT EXISTS` run before `BEGIN`, because
  Snowflake DDL auto-commits and would otherwise end the transaction early.
- Any exception triggers `ROLLBACK` and is re-raised (never silently treated),
  so the previous data is kept and Airflow correctly marks the task as failed.

## dbt

`dbt/README.md` has the full breakdown. Summary: `weather_daily` (ephemeral)
-> `weather_metrics` (ephemeral) -> `weather_analytics` (table), all window
functions `PARTITION BY city`. Metrics: daily average temperature, 7-day
moving average temperature, temperature anomaly, 7-day rolling
precipitation, dry spell length. Schema tests on `weather_analytics`:
`unique`/`not_null` on the city+date key, `not_null` on city/date,
`accepted_values` on city.

One snapshot (`weather_snapshot`, `check` strategy) tracks changes to the
raw weather columns (`temp_max`, `temp_min`, `precipitation`,
`weather_code`). A new version is recorded only when Open-Meteo revises a
day's values or a day ages out of the 61-day window. The computed metrics are
stored in the snapshot but don't trigger new versions, because they shift
whenever the window moves even when the underlying data hasn't changed.

## Snowflake tables

See `docs/report.md` Section 8 for full column-level detail. All tables are
created in the database set on the `snowflake_conn` Connection:

- `RAW.CITY_WEATHER_DAILY` -- raw landing table, PK `(city, date)`.
- `ANALYTICS.WEATHER_ANALYTICS` -- dbt's final table, source for BI.
- `SNAPSHOTS.WEATHER_SNAPSHOT` -- dbt snapshot of the analytics table.

## Dashboard

**Status: complete.** **Tool:** [Preset](https://preset.io) (cloud-hosted
Apache Superset). **Team/Workspace:** "DATA 226 Weather Analytics" /
"Weather Analytics Lab". **Dataset:** `ANALYTICS.WEATHER_ANALYTICS`.

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
   - Warehouse and Database: the same ones you set on `snowflake_conn`
   - Role: your role
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
cp .env.example .env                    # fill in SNOWFLAKE_KEY_DIR
```

`SNOWFLAKE_KEY_DIR` is the absolute path to the **folder** containing your
`rsa_key.p8` (not the path to the file itself). It is mounted read-only into
the containers at `/opt/airflow/snowflake_keys`.

### Required Airflow Variables

| Key | Value |
| --- | --- |
| `CITY_CONFIG` | The JSON shown in [Cities and configuration](#cities-and-configuration) |

### Required Airflow Connections

| Connection Id | Type | Notes |
| --- | --- | --- |
| `snowflake_conn` | Snowflake | Key-pair auth. Login = your Snowflake user, Password = your private key's passphrase, Account = your account identifier, Warehouse = your warehouse, Database = your database, Role = your role, Private Key (Path) = `/opt/airflow/snowflake_keys/rsa_key.p8`. Both DAGs read everything from this connection, so fill in every field. |

### How to run Airflow

```bash
docker compose up -d
docker compose exec airflow airflow dags list-import-errors   # should be empty
```

`docker-compose.yaml` maps the webserver to `${AIRFLOW_HOST_PORT:-8081}`, so
it defaults to `http://localhost:8081`. If 8081 is already in use on your
machine (for example by another course Airflow stack), set
`AIRFLOW_HOST_PORT=8082` in `.env`. Log in with `airflow` / `airflow` unless
you changed `_AIRFLOW_WWW_USER_*` in `.env`, then:

1. Add the Variable and Connection above.
2. Unpause `weather_dbt_dag`, then `weather_etl_dag`.

### How to run dbt

Either let `weather_dbt_dag` run it inside the container, or run it locally.
Outside Airflow, nothing sets the `DBT_*` variables that `profiles.yml`
reads, so export them first:

```bash
cd dbt
python3 -m venv venv && source venv/bin/activate
pip install dbt-core==1.8.7 dbt-snowflake==1.8.1

export DBT_ACCOUNT=your_account DBT_USER=your_user
export DBT_DATABASE=your_database DBT_WAREHOUSE=your_warehouse DBT_ROLE=your_role
export DBT_TYPE=snowflake
export DBT_PRIVATE_KEY_PATH="/absolute/path/to/rsa_key.p8"
export DBT_PRIVATE_KEY_PASSPHRASE=your_passphrase

dbt debug && dbt run && dbt test && dbt snapshot
```

## Security / secrets

- No password, private key, or account identifier is hardcoded anywhere in
  `dags/` or `dbt/`. `dbt/profiles.yml` is committed, but it only contains
  `env_var()` references.
- `.env` and any `*.p8`/`*.pub`/`*.pem` key file are gitignored (see
  `.gitignore`). Only `.env.example` with placeholder values is committed.
- The Snowflake private key lives on the host filesystem and is mounted
  **read-only** into the Airflow containers; it is never copied into the
  repo or pasted into a committed file. Its passphrase is stored only in the
  `snowflake_conn` Airflow Connection.
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

- Yunseo Oh (020746206)
- Isabella Shi (019256965)

Development was initially divided by city -- one member focused on Seoul,
the other on Toronto -- then merged into the single shared pipeline
described in this README (one Airflow DAG pair and one dbt project driven
by the `CITY_CONFIG` Variable, rather than a separate pipeline per city).
Both members reviewed the unified final pipeline before submission.
