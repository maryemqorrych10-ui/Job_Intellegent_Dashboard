from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="send_job_notifications",
    default_args=default_args,
    schedule_interval="0 8 * * *",   # tous les jours à 8h
    catchup=False,
) as dag:
    send_emails = BashOperator(
        task_id="send_emails",
        bash_command="python /opt/airflow/scripts/send_notifications.py",
    )