
from asyncio import tasks
from operators.postgres import DataFrameToPostgresOverrideOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate! 
from airflow.operators.python import PythonOperator
with DAG(
    'example_ml_dag',
    description='ML DAG',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['piotrek'],
) as dag:
    dag.doc_md = __doc__
    
    list_tables = PostgresOperator(
        task_id="check_if_table_exists",
        postgres_conn_id="postgres_default",
        sql="select * from pg_catalog.pg_tables;",
    )

    @tasks
    def check_if_table_exists(table_list, table: str):
        print(table_list)
        print(type(table_list))

    check_if_table_exists(table_list=list_tables.output, table="XD")