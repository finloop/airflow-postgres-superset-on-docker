import pendulum
from airflow.decorators import dag, task

from operators.postgres import (
    DataFrameToPostgresOverrideOperator,
    PostgresToDataFrameOperator,
)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["adrian"],
)

def order_items_reviews_dag():

    reviews = PostgresToDataFrameOperator(
        table_name="order_reviews",
        task_id="extract_order_reviews_dataset",
    )
    
    order_items = PostgresToDataFrameOperator(
        table_name="order_items",
        task_id="extract_order_items_dataset",
    )
    
    
    
    @task
    def fix_dates(reviews):
        import pandas as pd

        reviews.review_creation_date = pd.to_datetime(
            reviews.review_creation_date
        )
        reviews.review_answer_timestamp = pd.to_datetime(
            reviews.review_answer_timestamp
        )        
        return reviews     
     
    @task
    def order_items_2(order_items):
        import pandas as pd
        import numpy as np
        
        ordr_itms = order_items("order_id","product_id","seller_id","price","freight_value")
        
        return ordr_itms

    @task
    def merge(order_items,reviews):
        orderitems_reviews = reviews.merge(order_items, on="order_id")
        
        return orderitems_reviews

    order_reviews_fix = fix_dates(reviews.output)
    
    order_items_agg=order_items_2(order_items.output)
    
    merged = merge(
        reviews = order_reviews_fix,
        order_items = order_items_agg,
    )
    
    
    
    load = DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres",
        table_name="order_items_reviews",
        data=merged,
    )
    
my_dag = order_items_reviews_dag()