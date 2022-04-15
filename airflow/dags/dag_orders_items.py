import pendulum
from airflow.decorators import dag, task

from operators.postgres import DataFrameToPostgresOverrideOperator, PostgresToDataFrameOperator


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def seller_order_items_dag():
    sellers = PostgresToDataFrameOperator(
        table_name="sellers",
        task_id="extract_olist_sellers_dataset",
    )

    orders = PostgresToDataFrameOperator(
        table_name="order_items",
        task_id="extract_olist_order_items_dataset",
    )

    @task()
    def transform(df_sellers, df_orders):
        import pandas as pd

        df = df_orders.merge(
            df_sellers, left_on="seller_id", right_on="seller_id", how="inner"
        )
        return df
    
    transformed = transform(df_sellers=sellers.output, df_orders=orders.output)

    load = DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres",
        table_name="sellers_orders",
        data=transformed,
    )

    sellers >> transformed
    orders >> transformed
    transformed >> load


my_dag = seller_order_items_dag()