
from operators.postgres import DataFrameToPostgresOverrideOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag, task

import pendulum

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def ml_dag():
    list_tables = PostgresOperator(
        task_id="check_if_table_exists",
        postgres_conn_id="postgres_default",
        sql="select * from pg_catalog.pg_tables;",
    )
 
    @task()
    def check_if_table_exists(table_list, table: str):
        print(table_list)
        print(type(table_list))

    check_if_table_exists(table_list=list_tables.output, table="XD")