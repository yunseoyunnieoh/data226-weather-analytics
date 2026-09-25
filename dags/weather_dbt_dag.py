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

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt {{command}} --profiles-dir {DBT_PROJECT_DIR}"

with DAG(
    dag_id="weather_dbt_dag",
    description="dbt run -> dbt snapshot -> dbt test for the weather analytics models",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule=None,  # triggered only by weather_etl_dag, never on its own schedule
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data226_team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["DATA226", "weather", "dbt"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_CMD.format(command="run"),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=DBT_CMD.format(command="snapshot"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format(command="test"),
    )

    dbt_run >> dbt_snapshot >> dbt_test
