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

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import json
import requests


def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract():
    # CITY_CONFIG holds lat/long/timezone for each city
    cities = json.loads(Variable.get("CITY_CONFIG"))

    results = []
    for city, coords in cities.items():
        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "past_days": 60,
            "forecast_days": 1,
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "precipitation_sum",
                "weather_code"
            ],
            "timezone": coords["timezone"]
        }
        response = requests.get("https://api.open-meteo.com/v1/forecast", params=params)
        response.raise_for_status()
        results.append({
            "city": city,
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "daily": response.json()["daily"]
        })
    return results


@task
def transform(results):
    records = []
    for r in results:
        daily = r["daily"]
        for i in range(len(daily["time"])):
            records.append([
                r["city"],
                r["latitude"],
                r["longitude"],
                daily["time"][i],
                daily["temperature_2m_max"][i],
                daily["temperature_2m_min"][i],
                daily["precipitation_sum"][i],
                daily["weather_code"][i]
            ])
    return records


@task
def load(records, target_table):
    cur = return_snowflake_conn()

    cur.execute("CREATE SCHEMA IF NOT EXISTS RAW")

    # DDL auto-commits in Snowflake, so create tables before BEGIN.
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            city VARCHAR(64) NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            date DATE NOT NULL,
            temp_max FLOAT,
            temp_min FLOAT,
            precipitation FLOAT,
            weather_code INTEGER,
            loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (city, date)
        )
    """)

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {target_table}")
        for r in records:
            sql = f"""
                INSERT INTO {target_table}
                (city, latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, r)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id='weather_etl_dag',
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["DATA226", "weather", "etl", "multi-city"],
    schedule='30 2 * * *'
) as dag:
    target_table = "RAW.CITY_WEATHER_DAILY"

    results = extract()
    records = transform(results)
    load_task = load(records, target_table)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="weather_dbt_dag"
    )

    load_task >> trigger_dbt
