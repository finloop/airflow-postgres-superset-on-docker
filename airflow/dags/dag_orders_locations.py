import pendulum
from airflow.decorators import dag, task

from operators.postgres import DataFrameToPostgresOverrideOperator, PostgresToDataFrameOperator


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def orders_locations_dag():
    geolocations = PostgresToDataFrameOperator(
        table_name="geolocation",
        task_id="extract_olist_geolocation_dataset",
    )

    customers = PostgresToDataFrameOperator(
        table_name="customers",
        task_id="extract_olist_customers_dataset",
    )

    orders = PostgresToDataFrameOperator(
        table_name="orders",
        task_id="extract_olist_orders_dataset",
    )

    @task
    def fix_dates(orders):
        import pandas as pd
        orders.order_purchase_timestamp = pd.to_datetime(orders.order_purchase_timestamp)
        orders.order_approved_at = pd.to_datetime(orders.order_purchase_timestamp)
        orders.order_delivered_carrier_date = pd.to_datetime(orders.order_delivered_customer_date)
        orders.order_estimated_delivery_date = pd.to_datetime(orders.order_estimated_delivery_date)
        orders.order_delivered_customer_date = pd.to_datetime(orders.order_delivered_customer_date)
        return orders

    @task
    def aggregate_geolocations(geolocations):
        import pandas as np
        import numpy as np

        first = lambda x: x.iloc[0]
        geo = geolocations.groupby("geolocation_zip_code_prefix").agg(
            {
                "geolocation_zip_code_prefix": first,
                "geolocation_lat": np.mean,
                "geolocation_lng": np.mean,
            }
        )
        geo.index = geo.index.values
        geo.rename(
            columns={"geolocation_lat": "lat", "geolocation_lng": "lon"}, inplace=True
        )

        return geo

    @task 
    def merge(customers, orders, geolocations):
        customers_orders_geo = customers.merge(orders, on="customer_id").merge(
            geolocations,
            left_on=["customer_zip_code_prefix"],
            right_on=["geolocation_zip_code_prefix"],
            how="inner",
        )
        return customers_orders_geo

    fix_dates_res = fix_dates(orders.output)
    aggregate_geolocations_res = aggregate_geolocations(geolocations.output)
    merge_res = merge(customers=customers.output, orders=fix_dates_res, geolocations=aggregate_geolocations_res)

    load = DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres",
        table_name="orders_locations",
        data=merge_res,
    )


my_dag = orders_locations_dag()