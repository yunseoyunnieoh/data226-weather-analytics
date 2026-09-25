"""DATA 226 Team Lab: Weather Prediction Analytics (Seoul + Toronto).

Extracts daily weather for every city listed in the CITY_CONFIG Airflow
Variable from the Open-Meteo forecast API, then loads it into
DEV.RAW.CITY_WEATHER_DAILY with a transaction-safe MERGE (upsert), so the
same code handles any number of cities without a separate pipeline per city.

Adapted from the HW3 solution (weather_to_snowflake_airflow.py): same
TaskFlow (extract -> transform -> load) shape, same
BEGIN/verify/COMMIT/ROLLBACK transaction discipline, and the same
create-table-before-BEGIN ordering (Snowflake DDL auto-commits).

Airflow Variables:
    CITY_CONFIG - JSON object, e.g.
        {
          "Seoul":   {"latitude": 37.5665, "longitude": 126.9780, "timezone": "Asia/Seoul"},
          "Toronto": {"latitude": 43.6532, "longitude": -79.3832, "timezone": "America/Toronto"}
        }
Airflow Connections:
    snowflake_conn - Snowflake connection using key-pair authentication.
        No password, private key, or account details are hardcoded here.

On success, this DAG triggers weather_dbt_dag so dbt only ever runs on
freshly loaded data.
"""

import json
import logging
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


LOGGER = logging.getLogger(__name__)
TARGET_TABLE = "DEV.RAW.CITY_WEATHER_DAILY"
SNOWFLAKE_CONN_ID = "snowflake_conn"
PAST_DAYS = 60  # trailing window; enough history for 7-day windows + anomaly baseline
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "weather_code",
]


@task
def extract():
    """Call the Open-Meteo forecast API once per city in CITY_CONFIG."""
    city_config = json.loads(Variable.get("CITY_CONFIG"))
    if not city_config:
        raise ValueError("CITY_CONFIG Airflow Variable is empty; expected at least one city.")

    payloads = []
    for city, coords in city_config.items():
        latitude = float(coords["latitude"])
        longitude = float(coords["longitude"])
        timezone = coords["timezone"]
        if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
            raise ValueError(f"{city}: latitude/longitude out of range.")

        # forecast endpoint (not archive-api): past_days gives recent history,
        # forecast_days=1 includes today, in a single request per city.
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": latitude,
                "longitude": longitude,
                "daily": ",".join(DAILY_FIELDS),
                "timezone": timezone,
                "past_days": PAST_DAYS,
                "forecast_days": 1,
                "temperature_unit": "celsius",
                "precipitation_unit": "mm",
            },
            timeout=60,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise ValueError(f"{city}: Open-Meteo error: {data.get('reason', 'unknown')}")

        LOGGER.info("EXTRACT: city=%s latitude=%s longitude=%s days=%s",
                     city, latitude, longitude, len(data["daily"]["time"]))
        payloads.append({
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "daily": data["daily"],
        })
    return payloads


@task
def transform(payloads):
    """Flatten each city's daily arrays into (city, lat, lon, date, ...) rows."""
    records = []
    null_count = 0
    for payload in payloads:
        daily = payload["daily"]
        dates = daily.get("time", [])
        for field in DAILY_FIELDS:
            if len(daily.get(field, [])) != len(dates):
                raise ValueError(f"{payload['city']}: {field} length does not match time length.")

        for i, day in enumerate(dates):
            measurements = []
            for field in DAILY_FIELDS:
                value = daily[field][i]
                if value is None:
                    measurements.append(None)
                    null_count += 1
                elif field == "weather_code":
                    measurements.append(int(value))
                else:
                    measurements.append(float(value))
            records.append([
                payload["city"], payload["latitude"], payload["longitude"], day, *measurements,
            ])

    if not records:
        raise ValueError("No weather rows produced by transform; refusing to load an empty batch.")
    if null_count:
        LOGGER.warning("TRANSFORM: %s missing measurements kept as SQL NULL.", null_count)
    LOGGER.info("TRANSFORM: prepared %s rows across %s cities.", len(records), len(payloads))
    return records


@task
def load(records):
    """Upsert rows into DEV.RAW.CITY_WEATHER_DAILY inside one transaction.

    Idempotency: a MERGE keyed on (CITY, DATE) means rerunning the DAG for
    the same day updates that day's values in place instead of appending a
    duplicate row. Snowflake does not enforce the declared PRIMARY KEY, so a
    duplicate-key check runs before COMMIT as a second line of defense.
    """
    conn = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_conn()
    cur = None
    transaction_started = False
    try:
        cur = conn.cursor()

        # DDL auto-commits in Snowflake, so create tables before BEGIN.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS DEV.RAW.CITY_WEATHER_DAILY (
                CITY          VARCHAR(64)   NOT NULL,
                LATITUDE      FLOAT         NOT NULL,
                LONGITUDE     FLOAT         NOT NULL,
                DATE          DATE          NOT NULL,
                TEMP_MAX      FLOAT,
                TEMP_MIN      FLOAT,
                PRECIPITATION FLOAT,
                WEATHER_CODE  INTEGER,
                LOADED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (CITY, DATE)
            )
        """)
        cur.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS STG_CITY_WEATHER_DAILY (
                CITY VARCHAR(64), LATITUDE FLOAT, LONGITUDE FLOAT, DATE DATE,
                TEMP_MAX FLOAT, TEMP_MIN FLOAT, PRECIPITATION FLOAT, WEATHER_CODE INTEGER
            )
        """)

        cur.execute("BEGIN")
        transaction_started = True
        LOGGER.info("BEGIN: upserting %s rows into %s", len(records), TARGET_TABLE)

        cur.execute("DELETE FROM STG_CITY_WEATHER_DAILY")
        insert_staging_sql = """
            INSERT INTO STG_CITY_WEATHER_DAILY
                (city, latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
            VALUES (%s, %s, %s, TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s, %s)
        """
        for record in records:
            cur.execute(insert_staging_sql, tuple(record))

        cur.execute(f"""
            MERGE INTO {TARGET_TABLE} t
            USING STG_CITY_WEATHER_DAILY s
            ON t.city = s.city AND t.date = s.date
            WHEN MATCHED THEN UPDATE SET
                t.latitude = s.latitude,
                t.longitude = s.longitude,
                t.temp_max = s.temp_max,
                t.temp_min = s.temp_min,
                t.precipitation = s.precipitation,
                t.weather_code = s.weather_code,
                t.loaded_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (city, latitude, longitude, date, temp_max, temp_min, precipitation, weather_code, loaded_at)
                VALUES (s.city, s.latitude, s.longitude, s.date, s.temp_max, s.temp_min,
                        s.precipitation, s.weather_code, CURRENT_TIMESTAMP())
        """)

        # Snowflake standard-table primary keys are declarative only; verify
        # uniqueness explicitly before trusting the MERGE result.
        cur.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT city, date FROM {TARGET_TABLE}
                GROUP BY city, date HAVING COUNT(*) > 1
            ) AS duplicate_keys
        """)
        duplicate_count = cur.fetchone()[0]
        if duplicate_count:
            raise ValueError(f"Found {duplicate_count} duplicate (city, date) keys after MERGE.")

        cur.execute(f"SELECT city, COUNT(*), MIN(date), MAX(date) FROM {TARGET_TABLE} GROUP BY city")
        per_city_counts = cur.fetchall()

        cur.execute("COMMIT")
        transaction_started = False
        LOGGER.info("COMMIT succeeded: upsert completed for %s.", TARGET_TABLE)
        LOGGER.info("Duplicate (city, date) keys: %s", duplicate_count)
        for city, count, min_date, max_date in per_city_counts:
            LOGGER.info("  %s: %s rows, %s to %s", city, count, min_date, max_date)
        return {"rows_processed": len(records), "duplicate_keys": duplicate_count}
    except Exception:
        if transaction_started:
            try:
                cur.execute("ROLLBACK")
                LOGGER.error("ROLLBACK succeeded: previous table data preserved.")
            except Exception:
                LOGGER.exception("ROLLBACK failed; check the Snowflake session.")
        raise
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            conn.close()


with DAG(
    dag_id="weather_etl_dag",
    description="Open-Meteo -> Snowflake RAW for Seoul + Toronto, then triggers dbt",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="30 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data226_team",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["DATA226", "weather", "etl", "multi-city"],
) as dag:
    weather_payloads = extract()
    weather_records = transform(weather_payloads)
    load_result = load(weather_records)

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="weather_dbt_dag",
        wait_for_completion=False,
    )

    load_result >> trigger_dbt_dag
