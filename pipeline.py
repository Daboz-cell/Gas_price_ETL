import requests
import pandas as pd
from sqlalchemy import create_engine , text 
import psycopg2
import json
import http.client
import os
from dotenv import load_dotenv

# load the variables from my .env file (api key, db login info etc)
load_dotenv(dotenv_path="/root/Gas_price_ETL/.env")


# this function gets the gas price data from collectapi
def extract_city_prices():
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
    'content-type': "application/json",
    'authorization': os.getenv("COLLECTAPI_KEY")
    }

    # calling the state usa price endpoint for WA state
    conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

    res = conn.getresponse()
    data = res.read()
    # decode the bytes we got back into a normal string
    decoded_data = data.decode("utf-8")
    conn.close()
    return decoded_data


# this function takes the raw json and turns it into a clean dataframe
def transform_city_prices(decoded_data):
    # convert the json string into a python dict
    data_json = json.loads(decoded_data)
    
    # the cities list is nested inside result
    cities=data_json['result']['cities']

    # turn the list of cities into a dataframe
    city_prices_df = pd.DataFrame(cities)

    # rename some columns to make more sense
    city_prices_df.rename(columns={'name':'city_name','midGrade':'mid_grade'},inplace=True)

    # dont need this column so drop it
    city_prices_df.drop(columns=['lowername'],inplace=True)

    return city_prices_df


# this function loads the dataframe into postgres
def load_city_prices(city_prices_df):
    # get db credentials from the .env file
    DATABASE_NAME = os.getenv('DATABASE_NAME')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PORT = os.getenv('DATABASE_PORT')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE_HOST = os.getenv('DATABASE_HOST')

    # create the connection engine to the database
    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}')

    # quick check to print out whats already in the table (commented out for now)
    # with engine.connect() as conn:
    #     resort = conn.execute(text('select * from city_prices;'))
    #     for i in resort:
    #         print(i)

    # write the dataframe to the city_prices table, replace if it already exists
    city_prices_df.to_sql('city_prices',engine , if_exists='replace',index=False )

# main function that runs everything in order
def main():
    decoded_data=extract_city_prices()
    city_prices_df=transform_city_prices(decoded_data)
    load_city_prices(city_prices_df)

    print('ETL process completed successfully.')

if __name__ == "__main__":
    main()