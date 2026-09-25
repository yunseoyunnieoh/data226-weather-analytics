# dbt project: weather_analytics

Transforms `DEV.RAW.CITY_WEATHER_DAILY` (loaded by `dags/weather_etl_dag.py`)
into per-city weather analytics for Seoul and Toronto.

## Layers

| Layer | Model | Materialization | Purpose |
| --- | --- | --- | --- |
| staging | `stg_weather` | view | Pass-through cleanup + `city_date` surrogate key |
| intermediate | `int_weather_metrics` | ephemeral | Daily average temp, dry spell length |
| analytics | `weather_analytics` | table | Moving avg temp, temp anomaly, rolling precipitation, dry spell length -- read by the BI dashboard |

All window functions are `PARTITION BY city` so Seoul and Toronto are never
combined in the same calculation.

## Snapshot

`snapshots/weather_snapshot.sql` snapshots `weather_analytics` using the
`check` strategy (not `timestamp`, since there is no natural `updated_at`
column and Open-Meteo revises very recent days as more observations arrive).
It tracks how each `(city, date)` row's metrics change between dbt runs.

## Tests

Defined in `models/schema.yml`:

- `unique` + `not_null` on `city_date` (the composite city+date key) for
  both `stg_weather` and `weather_analytics`
- `not_null` on `city` and `weather_date`
- `accepted_values` on `city` (`Seoul`, `Toronto`)

## Running locally (outside Docker)

```bash
python3 -m venv venv
source venv/bin/activate
pip install dbt-core==1.8.7 dbt-snowflake==1.8.1

cp profiles.yml.example profiles.yml   # then edit with your own account/user/key path
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your_passphrase

dbt debug
dbt run
dbt snapshot
dbt test
```

## Running through Airflow

`dags/weather_dbt_dag.py` runs the same three commands inside the Airflow
container, against the project mounted at `/opt/airflow/dbt`. It is
triggered by `weather_etl_dag` after a successful load -- see the repo root
`README.md` for the full orchestration picture.
