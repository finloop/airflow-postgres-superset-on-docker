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

def orders_info_dag():

    products = PostgresToDataFrameOperator(
        table_name="products",
        task_id="extract_products_dataset",
    )
    
    product_category_names = PostgresToDataFrameOperator(
        table_name="product_category_names",
        task_id="extract_product_category_names_dataset",
    )
    
    orders = PostgresToDataFrameOperator(
        table_name="orders",
        task_id="extract_orders_dataset",
    )
    
    order_items = PostgresToDataFrameOperator(
        table_name="order_items",
        task_id="extract_order_items_dataset",
    )
    
    @task
    def fix_date(orders):
        import pandas as pd

        orders.order_purchase_timestamp = pd.to_datetime(
            orders.order_purchase_timestamp
        )
        
        return orders
    
    @task
    def eng_name_prod(products, product_category_names):
        import pandas as pd
        
        products=products.drop(["product_name_lenght","product_description_lenght","product_photos_qty"],axis=1)
        
        eng_name_prod = products.merge(product_category_names, on="product_category_name")
        
        eng_name_prod = eng_name_prod.drop("product_category_name", axis=1)
        
        return eng_name_prod
        
    @task
    def order_price(orders, order_items):
        import pandas as pd
        
        orders = orders.drop(["order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date"],axis=1)
        
        order_items = order_items.drop(["order_item_id","seller_id","shipping_limit_date"],axis=1)
        
        order_price = orders.merge(order_items, on="order_id")
        
        return order_price
        
        
    @task
    def order_info(order, product_name):
        import pandas as pd
    
        order.merge(product_name, on="product_id")
        return order_info
    
    
    orders_fix=fix_date(orders=orders.output)
    products_eng = eng_name_prod(products=products.output, product_category_names=product_category_names.output)
    order_prices=order_price(orders=orders_fix, order_items=order_items.output)
    orders_info=order_info(order=order_prices, product_name=products_eng)
    
    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_english_product_category_name_result",
        table_name="products_eng",
        data=products_eng,
    )
    
    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_order_prices_result",
        table_name="orders_and_prices",
        data=order_prices,
    )
    
    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_orders_info_result",
        table_name="orders_information",
        data=orders_info,
    )
    
my_dag = orders_info_dag()