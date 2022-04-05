from collections import _OrderedDictItemsView
import pandas as pd
from sqlalchemy import create_engine

customers = pd.read_csv("olist_customers_dataset.csv")

geolocation = pd.read_csv("olist_geolocation_dataset.csv")

order_items = pd.read_csv("olist_order_items_dataset.csv")

order_payments = pd.read_csv("olist_order_payments_dataset.csv")

order_reviews = pd.read_csv("olist_order_reviews_dataset.csv")

orders = pd.read_csv("olist_orders_dataset.csv")

products = pd.read_csv("olist_products_dataset.csv")

sellers = pd.read_csv("olist_sellers_dataset.csv")

product_category_names = pd.read_csv("product_category_name_translation.csv")


engine = create_engine('postgresql://postgres:postgres@localhost:5051/postgres')


customers.to_sql(
    'customers', 
    con=engine,
    if_exists="replace",
    index=False
)

geolocation.to_sql(
    'geolocation', 
    con=engine,
    if_exists="replace",
    index=False
)

order_items.to_sql(
    'order_items', 
    con=engine,
    if_exists="replace",
    index=False
)

order_payments.to_sql(
    'order_payments', 
    con=engine,
    if_exists="replace",
    index=False
)

order_reviews.to_sql(
    'order_reviews', 
    con=engine,
    if_exists="replace",
    index=False
)

orders.to_sql(
    'orders', 
    con=engine,
    if_exists="replace",
    index=False
)

products.to_sql(
    'products', 
    con=engine,
    if_exists="replace",
    index=False
)

sellers.to_sql(
    'sellers', 
    con=engine,
    if_exists="replace",
    index=False
)

product_category_names.to_sql(
    'product_category_names', 
    con=engine,
    if_exists="replace",
    index=False
)

