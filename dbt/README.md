# dbt project: weather_analytics

Transforms `RAW.CITY_WEATHER_DAILY` (loaded by `dags/weather_etl_dag.py`)
into per-city weather analytics for Seoul and Toronto.

## Layers

| Layer | Model | Materialization | Purpose |
| --- | --- | --- | --- |
| transform | `weather_daily` | ephemeral | Pass-through cleanup + `city_date` surrogate key |
| transform | `weather_metrics` | ephemeral | Daily average temp, dry spell length |
| analytics | `weather_analytics` | table | Moving avg temp, temp anomaly, rolling precipitation, dry spell length -- read by the BI dashboard |

All window functions are `PARTITION BY city` so Seoul and Toronto are never
combined in the same calculation.

## Snapshot

`snapshots/weather_snapshot.sql` snapshots `weather_analytics` using the
`check` strategy (not `timestamp`, since there is no natural `updated_at`
column and Open-Meteo revises very recent days as more observations arrive).

`check_cols` only includes the raw weather columns (`temp_max`, `temp_min`,
`precipitation`, `weather_code`). The computed metrics are still stored in
every snapshot row, but they don't trigger new versions. They shift whenever
the 61-day window moves (for example, `temp_anomaly` is measured against the
city's average over the whole window), so checking them would create a new
version of every row on every run. A new version is recorded only when
Open-Meteo revises a day's values or a day ages out of the window.

## Tests

Defined in `models/schema.yml`:
- `unique` + `not_null` on `city_date` (the composite city+date key)
- `not_null` on `city` and `weather_date`
- `accepted_values` on `city` (`Seoul`, `Toronto`)

## Running locally (outside Docker)

`profiles.yml` reads every connection setting from `DBT_*` environment
variables. Airflow sets these automatically; locally, export them first:

```bash
python3 -m venv venv
source venv/bin/activate
pip install dbt-core==1.8.7 dbt-snowflake==1.8.1

export DBT_ACCOUNT=your_account DBT_USER=your_user
export DBT_DATABASE=your_database DBT_WAREHOUSE=your_warehouse DBT_ROLE=your_role
export DBT_TYPE=snowflake
export DBT_PRIVATE_KEY_PATH="/absolute/path/to/rsa_key.p8"
export DBT_PRIVATE_KEY_PASSPHRASE=your_passphrase

dbt debug
dbt run
dbt test
dbt snapshot
```

## Running through Airflow

`dags/weather_dbt_dag.py` runs `dbt run -> dbt test -> dbt snapshot` inside
the Airflow container, against the project mounted at `/opt/airflow/dbt`. It is
triggered by `weather_etl_dag` after a successful load -- see the repo root
`README.md` for the full orchestration picture.
