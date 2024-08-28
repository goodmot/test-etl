from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'asukhikh',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag_with_postgres_operator',
    default_args=default_args,
    start_date=datetime(2024, 8, 26),
    schedule_interval='0 0 * * *'
) as dag:

    start = EmptyOperator(task_id='start', wait_for_downstream=True)

    task1 = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_local',  # host: host.docker.internal
        sql="""
            CREATE TABLE IF NOT EXISTS orders (
                order_id int,
                status varchar(50)
            );
        """
    )

    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_local',
        sql="""
            INSERT INTO orders
            SELECT 
                floor(random() * 1000000) as order_id,
                (array['created', 'in transit', 'delivered'])[floor(random() * 3 + 1)] as status
            FROM generate_series(1, 1);
        """
    )

    finish = EmptyOperator(task_id='finish')

    start >> task1 >> task2 >> finish
