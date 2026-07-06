from datetime import datetime, timedelta
import http.client
import json
import os
import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# imports: dag and task decorators from airflow's TaskFlow API to define the pipeline,
# http.client and json to fetch and parse the API response, os to access environment variables,
# pandas for data manipulation, dotenv to load credentials from the .env file,
# and sqlalchemy to connect to PostgreSQL


# use absolute path so Airflow can find the .env file
load_dotenv(dotenv_path="/root/Gas_price_ETL/.env")

# define the DAG using the TaskFlow API decorator
@dag(
    dag_id="gas_prices_taskflow_dag",
    start_date=datetime(2026, 6, 10),
    schedule=timedelta(hours=1),
    catchup=False,
)
def gas_prices_etl():
    # extract gas price data for Washington state cities from CollectAPI
    @task
    def extract():
        # open an HTTPS connection to the CollectAPI server
        conn = http.client.HTTPSConnection("api.collectapi.com")
        headers = {
        "content-type": "application/json",
        "authorization": os.getenv("COLLECTAPI_KEY"),
        }
        # calling the state usa price endpoint for WA state
        conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)
        res = conn.getresponse()
        # decode the response bytes into a python dict and extract the cities list
        data = json.loads(res.read().decode("utf-8"))
        data = data["result"]["cities"]
        conn.close()
        # return the cities list — Airflow automatically pushes this to XCom
        return data

    # this function cleans and restructures the raw cities data
    @task
    def transform(cities):
        df = pd.DataFrame(cities)
        df.rename(columns={"name": "city_name", "midGrade": "mid_grade"}, inplace=True)
        df.drop(columns=["lowername"], inplace=True, errors="ignore")
        # replace NaN values with None so PostgreSQL can store them properly
        df = df.astype(object).where(pd.notna(df), None)
        return df.to_dict(orient="records")

    # this function loads the cleaned data into PostgreSQL database
    @task
    def load(city_records):
        df = pd.DataFrame(city_records)
        # build the SQLAlchemy engine using database credentials from the .env file
        engine = create_engine(
            f"postgresql+psycopg2://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@"
            f"{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
        )
         # test the database connection
        with engine.connect() as conn:
            result = conn.execute(text("select 1;"))
            for row in result:
                print(row)
        df.to_sql("city_prices", engine, if_exists="replace", index=False)
        print(f"ETL completed. {len(df)} records loaded.")

    # wire up the tasks — TaskFlow automatically handles XCom between them
    load(transform(extract()))

gas_prices_etl()
