from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'asukhikh',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag_with_clickhouse_operator',
    default_args=default_args,
    start_date=datetime(2024, 8, 26),
    schedule_interval='0 0 * * *'
) as dag:
    start = EmptyOperator(task_id='start', wait_for_downstream=True)

    # sensor from postgres

    # create click-postgres table

    # create click table if not exist

    # insert

    # delete click-postgres table

    finish = EmptyOperator(task_id='finish')