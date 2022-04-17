
from operators.postgres import CheckIfTableExistsOperator
from airflow.decorators import dag, task

import pendulum

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def ml_dag():
    check1 = CheckIfTableExistsOperator(table_name="customers", task_id="table_customers_exist")

my_dag = ml_dag()
