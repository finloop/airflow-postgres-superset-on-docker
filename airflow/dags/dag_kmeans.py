import pendulum
from airflow.decorators import dag, task
from typing import Dict

from operators.postgres import (
    CheckIfTableExistsOperator,
    PostgresToDataFrameOperator,
    DataFrameToPostgresOverrideOperator,
)

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["adrian"],
)
def kmeans_dag():
    geolocations = PostgresToDataFrameOperator(
        table_name="orders_locations",
        connection_uri="postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        task_id="get_orders_locations_table",
    )
        

    @task
    def  kmeans_task(coordinate):
        import pandas as pd
        from sklearn.cluster import KMeans

        #geolocation = geolocations.toPandas()
    
        coordinates = coordinate[["lat","lon"]]

        kmeans = KMeans(
        init="random",
        n_clusters=24,
        n_init=10,
        max_iter=300,
        random_state=42
        ).fit(coordinates)

        centers = kmeans.cluster_centers_
        centra = pd.DataFrame({"lat": centers[:,0],"lon": centers[:,1]})
        return centra

    kmeans_result = kmeans_task(geolocations.output)
    save_centers = DataFrameToPostgresOverrideOperator(
        table_name="geolocation_cluster_centers", 
        task_id="save_cluster_centers", 
        data=kmeans_result,
    )

my_dag = kmeans_dag()