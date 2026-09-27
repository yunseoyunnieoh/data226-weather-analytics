"""DATA 226 Team Lab: dbt orchestration DAG.

Runs the dbt project against DEV.RAW.CITY_WEATHER_DAILY: dbt run builds the
staging/intermediate/analytics models, dbt snapshot captures the analytics
table's current state for change tracking, and dbt test validates the
result. This DAG is never scheduled on its own -- weather_etl_dag triggers
it (see trigger_dbt_dag task there) so dbt always runs after a fresh load,
never independently or before the ETL succeeds.

The dbt project and its profiles.yml (gitignored, holds Snowflake key-pair
config, never committed) are mounted into the Airflow containers at
/opt/airflow/dbt by docker-compose.yaml.
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/dbt"

conn = BaseHook.get_connection('snowflake_conn')

with DAG(
    dag_id="weather_dbt_dag",
    description="dbt run -> dbt test -> dbt snapshot for the weather analytics models",
    start_date=datetime(2026, 9, 1),
    schedule=None,  # triggered only by weather_etl_dag, never on its own schedule
    catchup=False,
    max_active_runs=1,
    default_args={
        "env": {
            "DBT_USER": conn.login,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_PRIVATE_KEY_PASSPHRASE": conn.password,
            "DBT_PRIVATE_KEY_PATH": conn.extra_dejson.get("private_key_file"),
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_ROLE": conn.extra_dejson.get("role", "ACCOUNTADMIN"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
    tags=["DATA226", "weather", "dbt"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test >> dbt_snapshot
