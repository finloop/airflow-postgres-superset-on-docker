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
    tags=["piotrek"],
)
def customers_dag():
    customers = PostgresToDataFrameOperator(
        table_name="customers",
        task_id="extract_customers_dataset",
    )

    orders = PostgresToDataFrameOperator(
        table_name="orders",
        task_id="extract_olist_orders_dataset_dataset",
    )

    @task
    def merge_orders_customers(customers, orders):
        import pandas as pd

        df_orders_customers = orders.merge(customers, on="customer_id")
        return df_orders_customers

    @task()
    def orders_num_per_customer(df_orders_customers):
        import pandas as pd

        df_orders_num_per_customer = (
            df_orders_customers.groupby("customer_unique_id").count().iloc[:, 0]
        )
        df_orders_num_per_customer = pd.DataFrame(df_orders_num_per_customer)
        df_orders_num_per_customer.columns = ["orders_num_per_customer"]

        return df_orders_num_per_customer

    @task
    def group_by_order_count(df_orders_num_per_customer):
        import pandas as pd

        def fix(x):
            return x[1]

        customers_with_order_count = pd.DataFrame(
            df_orders_num_per_customer.value_counts()
        )
        customers_with_order_count.columns = ["number_of_customers"]
        customers_with_order_count.index = (
            customers_with_order_count.index.map(str).map(fix) + " zamówienie"
        )
        return customers_with_order_count

    @task
    def add_returing_column(customers, orders_num_per_customer):
        import pandas as pd

        customers["returing"] = customers.customer_unique_id.map(
            orders_num_per_customer.iloc[:, 0] > 1
        ).astype("int32")

        return customers

    @task
    def fix_dates(orders):
        import pandas as pd

        orders.order_purchase_timestamp = pd.to_datetime(
            orders.order_purchase_timestamp
        )
        orders.order_approved_at = pd.to_datetime(orders.order_purchase_timestamp)
        orders.order_delivered_carrier_date = pd.to_datetime(
            orders.order_delivered_customer_date
        )
        orders.order_estimated_delivery_date = pd.to_datetime(
            orders.order_estimated_delivery_date
        )
        orders.order_delivered_customer_date = pd.to_datetime(
            orders.order_delivered_customer_date
        )
        return orders

    fix_dates_res = fix_dates(orders=orders.output)
    merge_orders_customers_result = merge_orders_customers(
        customers=customers.output, orders=fix_dates_res
    )
    orders_num_per_customer_result = orders_num_per_customer(
        df_orders_customers=merge_orders_customers_result
    )
    group_by_order_count_result = group_by_order_count(
        df_orders_num_per_customer=orders_num_per_customer_result
    )
    add_returing_column_result = add_returing_column(
        customers=merge_orders_customers_result,
        orders_num_per_customer=orders_num_per_customer_result,
    )

    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_group_by_order_count_result",
        table_name="group_by_order_count_result",
        data=group_by_order_count_result,
    )

    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_orders_num_per_customer_result",
        table_name="orders_num_per_customer",
        data=orders_num_per_customer_result,
    )

    DataFrameToPostgresOverrideOperator(
        task_id="upload_to_postgres_customers_result",
        table_name="customers",
        data=add_returing_column_result,
    )


my_dag = customers_dag()
