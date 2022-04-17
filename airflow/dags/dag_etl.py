import json
from textwrap import dedent
from operators.postgres import PostgresToDataFrameOperator

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

with DAG(
    "example_dag_with_postgres",
    description="ETL DAG that extracts data from postgres",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
) as dag:
    dag.doc_md = __doc__

    customers = PostgresToDataFrameOperator(
        table_name="customers", task_id="get_customers_table"
    )
