import os
import pathlib
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
DBT_PROFILES_DIR = PROJECT_ROOT / "profiles"


default_args = {
    "owner": "data-eng",
}

with DAG(
    dag_id="dbt_daily_balance_alert",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # No local warehouse needed for Snowflake
    ensure_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=lambda: None,
    )

    # dbt seed + run + test
    dbt_env = {
        "DBT_PROFILES_DIR": str(DBT_PROFILES_DIR),
        "PATH": os.environ.get("PATH", ""),
    }

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --full-refresh | cat",
        env=dbt_env,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run | cat",
        env=dbt_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test | cat",
        env=dbt_env,
    )

    # Alert task
    def _alert():
        from utils.alerts import run_daily_balance_alerts

        run_daily_balance_alerts()

    alert = PythonOperator(
        task_id="alert_daily_balance_change",
        python_callable=_alert,
        env={
            "PAGERDUTY_ROUTING_KEY": os.environ.get("PAGERDUTY_ROUTING_KEY", ""),
            "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
            "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", ""),
            "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
            "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", ""),
            "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", ""),
        },
    )

    ensure_dirs >> dbt_seed >> dbt_run >> dbt_test >> alert


