from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import BaseOperator


class DataFrameToPostgresOverrideOperator(BaseOperator):
    """
    Saves pd.DataFrame to postgres table.
    """

    template_fields = ("data",)

    def __init__(
        self,
        table_name,
        data,
        connection_uri: str = "postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        *args,
        **kwargs,
    ):
        super(DataFrameToPostgresOverrideOperator, self).__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name
        self.data = data

    def execute(self, context):
        import pandas as pd
        from sqlalchemy import create_engine

        con = create_engine(self.connection_uri)
        self.data.to_sql(name=self.table_name, con=con, if_exists="replace")


class PostgresToDataFrameOperator(BaseOperator):
    """
    Load pd.DataFrame from postgres table.
    """

    def __init__(
        self,
        table_name,
        connection_uri: str = "postgresql://postgres:postgres@client-postgres:5432/postgres",
        *args,
        **kwargs,
    ):
        super(PostgresToDataFrameOperator, self).__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name

    def execute(self, context):
        import pandas as pd
        from sqlalchemy import create_engine

        con = create_engine(self.connection_uri)
        # WARNING. Never use this in production, it can be easly exploited with
        # SQL injection.
        df = pd.read_sql_query(f"select * from {self.table_name}", con=con)
        return df


class CheckIfTableExistsOperator(BaseOperator):
    def __init__(
        self,
        table_name,
        connection_uri: str = "postgresql://postgres:postgres@warehouse-postgres:5432/postgres",
        *args,
        **kwargs,
    ):
        super(CheckIfTableExistsOperator, self).__init__(*args, **kwargs)
        self.connection_uri = connection_uri
        self.table_name = table_name

    def execute(self, context):
        import pandas as pd
        import numpy as np
        from sqlalchemy import create_engine

        con = create_engine(self.connection_uri)
        # WARNING. Never use this in production, it can be easly exploited with
        # SQL injection.
        df = pd.read_sql_query(f"select * from pg_catalog.pg_tables;", con=con)
        if np.sum(df["tablename"] == self.table_name) == 0:
            raise AirflowFailException(f"Table {self.table_name} does not exist.")
