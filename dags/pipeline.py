import pendulum
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from airflow.exceptions import AirflowSkipException
from airflow.sdk import task, dag


def get_engine(url: str):
    return create_engine(url=url)


@dag(
    dag_id="aapl_pipeline",
    description="airflow pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 24),
    catchup=False,
    tags=["yfinance", "postgres"],
)
def aapl_pipeline():
    # task 1
    @task
    def get_data() -> dict:
        data = yf.download(["AAPL"], period="1d")

        if isinstance(data, pd.DataFrame):
            if data.empty: # weekend / market holiday
                raise AirflowSkipException("yfinance returned no data")

            # XCom needs plain data, so split the DataFrame into 3 simple pieces
            return {
                "index": [d.strftime("%Y-%m-%d") for d in data.index],
                "columns": [list(col) for col in data.columns],
                "data": data.values.tolist(),
            }
        
        else:
            raise AirflowSkipException("yfinance returned no data")


    # task 2
    @task
    def transform_columns(data: dict) -> list[dict]:
        # rebuild the DataFrame from the 3 pieces
        df = pd.DataFrame(
            data=data["data"],
            index=pd.Index(data["index"], name="Date"),
            columns=pd.MultiIndex.from_tuples(data["columns"]),
        )
        df.columns = [f"{price}_{ticker}" for price, ticker in df.columns]
        df = df.reset_index()  # turn the Date index into a normal column
        return df.to_dict(orient="records") 
    
    # task 3
    @task
    def push_to_database(records: list[dict], table_name: str) -> None:
        df = pd.DataFrame(records)
        df["Date"] = pd.to_datetime(df["Date"]) # convert datetime

        engine = get_engine(
            url="postgresql+psycopg://postgres:1234@interesting_boyd:5432/postgres"
        )

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
        )

    data = get_data()
    records = transform_columns(data) # type: ignore
    push_to_database(records, "aapl") # type: ignore


aapl_pipeline()