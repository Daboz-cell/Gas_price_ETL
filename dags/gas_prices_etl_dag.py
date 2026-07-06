from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import http.client
import json
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# imports: DAG and PythonOperator to define the pipeline and run python functions,
# http.client and json to fetch and parse the API response, pandas for data manipulation,
# sqlalchemy to connect to PostgreSQL, os to access environment variables,
# and dotenv to load credentials from the .env file

# use absolute path so Airflow can find the .env file
load_dotenv(dotenv_path="/root/Gas_price_ETL/.env")

# extract gas price data for Washington state cities from CollectAPI
def extract_cities(**kwargs):
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
def transform_cities(**kwargs):
    # pull the cities list returned by the extract task via XCom
    data = kwargs["ti"].xcom_pull(task_ids="extract")
    cities_df = pd.DataFrame(data)
    cities_df.drop(columns="lowername", inplace=True, errors="ignore")
    cities_df.rename(columns={"name": "city_name", "midGrade": "mid_grade"}, inplace=True)
    # replace NaN values with None so PostgreSQL can store them properly
    cities_df = cities_df.astype(object).where(pd.notna(cities_df), None)
    # convert the dataframe to a list of dicts so XCom can serialize it
    cities_records = cities_df.to_dict(orient="records")
    # manually push the transformed records to XCom with a custom key
    kwargs["ti"].xcom_push(key="transform", value=cities_records)


# this function loads the cleaned data into  PostgreSQL database
def load_cities(**kwargs):
    # pull the transformed records from XCom using the custom key
    cities_records = kwargs["ti"].xcom_pull(task_ids="transform", key="transform")
    cities_df = pd.DataFrame(cities_records)
    # build the SQLAlchemy engine using  database credentials from the .env file
    engine = create_engine(
        f"postgresql+psycopg2://{os.getenv('DATABASE_USER')}:{os.getenv('DATABASE_PASSWORD')}@"
        f"{os.getenv('DATABASE_HOST')}:{os.getenv('DATABASE_PORT')}/{os.getenv('DATABASE_NAME')}"
    )
    # test the database connection
    with engine.connect() as conn:
        result = conn.execute(text("select 1;"))
        for row in result:
            print(row)
    # write the dataframe into the city_prices table, replacing it if it already exists
    cities_df.to_sql("city_prices", engine, if_exists="replace", index=False)

with DAG(
    "gas_prices_etl_dag",
    # the date from which Airflow starts scheduling runs
    start_date=datetime(2026, 6, 10),
    # run this pipeline every 1 hour
    schedule=timedelta(hours=1),
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract_cities)
    transform_task = PythonOperator(task_id="transform", python_callable=transform_cities)
    load_task = PythonOperator(task_id="load", python_callable=load_cities)
    # set the task execution order: extract first, then transform, then load
    extract_task >> transform_task >> load_task
