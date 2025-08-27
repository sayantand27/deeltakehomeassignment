import os
import pathlib
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Adjust this if your project structure differs
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
DBT_PROFILES_DIR = PROJECT_ROOT / "profiles"

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="dbt_daily_balance_alert",
    default_args=default_args,
    description="Run dbt + send alerts for >50% balance changes",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "alerts", "balance"],
) as dag:

    # Dummy step for flexibility (Airflow best practice)
    ensure_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=lambda: None,
    )

    # DBT environment
    dbt_env = {
        "DBT_PROFILES_DIR": str(DBT_PROFILES_DIR),
        "PATH": os.environ.get("PATH", ""),
    }

    # Run dbt seed
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="dbt seed --full-refresh",
        env=dbt_env,
    )

    # Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run",
        env=dbt_env,
    )

    # Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test",
        env=dbt_env,
    )

    # Python alert task for >50% balance change
    def _alert():
        from utils.alerts import run_daily_balance_alerts  # Make sure this path is correct
        run_daily_balance_alerts()

    alert = PythonOperator(
        task_id="alert_daily_balance_change",
        python_callable=_alert,
        env={
            "PAGERDUTY_ROUTING_KEY": os.environ.get("PAGERDUTY_ROUTING_KEY", ""),
            "SLACK_WEBHOOK_URL": os.environ.get("SLACK_WEBHOOK_URL", ""),
            "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT", ""),
            "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER", ""),
            "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD", ""),
            "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE", ""),
            "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE", ""),
            "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE", ""),
            "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", ""),
        },
    )

    # DAG execution flow
    ensure_dirs >> dbt_seed >> dbt_run >> dbt_test >> alert
