from operators.postgres import (
    CheckIfTableExistsOperator,
    PostgresToDataFrameOperator,
    DataFrameToPostgresOverrideOperator,
)
from airflow.decorators import dag, task
from typing import Dict


import pendulum


@task()
def clean_data(df):
    """
    Cleans the dataset.
    """
    import pandas as pd

    df.set_index("order_purchase_timestamp", inplace=True)
    X = df.resample("1d").index.count()
    Y = X.loc["2017-02-01":"2018-08-20"]
    return Y


@task(multiple_outputs=True)
def train_model(Y):
    from ThymeBoost import ThymeBoost as tb

    model = tb.ThymeBoost(
        approximate_splits=True, n_split_proposals=25, verbose=0, cost_penalty=0.001
    )

    output = model.fit(
        Y,
        trend_estimator="linear",
        seasonal_estimator="fourier",
        seasonal_period=365,
        split_cost="mse",
        global_cost="maicc",
        fit_type="global",
    )
    output["pred"] = "No"
    return {"model": model, "fit": output}


@task()
def predict(model, output):
    from ThymeBoost import ThymeBoost as tb

    predicted_output = model.predict(output, 300)
    output = output.reset_index()
    predicted_output["order_purchase_timestamp"] = predicted_output.index.values
    predicted_output["pred"] = "Yes"
    predicted_output = predicted_output.rename(
        columns={
            "predictions": "y",
            "predicted_trend": "trend",
            "predicted_seasonality": "seasonality",
            "predicted_upper": "yhat_upper",
            "predicted_lower": "yhat_lower",
        }
    )
    return output.merge(predicted_output, how="outer")


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["piotrek"],
)
def ml_dag():
    check1 = CheckIfTableExistsOperator(
        table_name="customers", task_id="table_customers_exist"
    )
    customers = PostgresToDataFrameOperator(
        table_name="customers",
        connection_uri="postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        task_id="get_customers_table",
    )

    cleaned_data = clean_data(df=customers.output)
    train_out = train_model(Y=cleaned_data)
    predict_out = predict(model=train_out["model"], output=train_out["fit"])
    save_fit = DataFrameToPostgresOverrideOperator(
        table_name="fit", task_id="save_fit", data=train_out["fit"]
    )
    save_predict = DataFrameToPostgresOverrideOperator(
        table_name="predict", task_id="save_predict", data=predict_out
    )

    check1 >> customers


my_dag = ml_dag()
