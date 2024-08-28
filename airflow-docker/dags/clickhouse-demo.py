from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

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
    is_data_in_postgres = ExternalTaskSensor(
        task_id='is_data_in_postgres',
        external_dag_id='dag_with_postgres_operator',
        external_task_id='finish'
    )

    # create click-postgres table
    create_table_postgres_click = ClickHouseOperator(
        task_id='create_table_postgres_click',
        clickhouse_conn_id='clickhouse_local',  # host: host.docker.internal
        sql="""
            CREATE TABLE airflow_click.orders_tmp (
                order_id Int64,
                status String
            )
            ENGINE = PostgreSQL('172.27.0.3:5432', 'airflow', 'orders', 'airflow', 'airflow', 'public');
        """
    )

    # create click table if not exist
    create_table_click = ClickHouseOperator(
        task_id='create_table_click',
        clickhouse_conn_id='clickhouse_local',  # host: host.docker.internal
        sql="""
            CREATE TABLE IF NOT EXISTS airflow_click.orders (
                order_id Int64,
                status String
            )
            ENGINE = MergeTree()
            ORDER BY order_id;
        """
    )

    # truncate
    truncate_table_click = ClickHouseOperator(
        task_id='truncate_table_click',
        clickhouse_conn_id='clickhouse_local',  # host: host.docker.internal
        sql="""
            TRUNCATE TABLE airflow_click.orders;
        """
    )

    # insert
    insert_into_click = ClickHouseOperator(
        task_id='insert_into_click',
        clickhouse_conn_id='clickhouse_local',  # host: host.docker.internal
        sql="""
        INSERT INTO airflow_click.orders
        SELECT * FROM airflow_click.orders_tmp;
        """
    )

    # drop click-postgres table
    drop_table_postgres_click = ClickHouseOperator(
        task_id='drop_table_postgres_click',
        clickhouse_conn_id='clickhouse_local',  # host: host.docker.internal
        sql='''
        DROP TABLE airflow_click.orders_tmp;
        '''
    )

    finish = EmptyOperator(task_id='finish')

    start >> is_data_in_postgres >> \
          create_table_postgres_click >> create_table_click >> truncate_table_click >> \
          insert_into_click >> drop_table_postgres_click >> finish
