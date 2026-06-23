import requests
import pandas as pd
from sqlalchemy import create_engine , text
import os
from dotenv import load_dotenv

load_dotenv()


def extract_articles():
    url = ('https://newsapi.org/v2/everything?'
            'q=Apple&'
            'from=2026-06-10&to=2026-06-11&'
            'sortBy=popularity&'
            f'apiKey={os.getenv('NEWS_API_KEY')}')

    response=requests.get(url)

    data=response.json()

    return data

def transform_articles(data):
    articles_list=data['articles']

    articles_df=pd.DataFrame(articles_list)

    articles_df.drop(columns=['source','urlToImage'],inplace=True)

    articles_df.rename(columns={'publishedAt':'published_at'},inplace=True)

    return articles_df

def load_articles(articles_df):

    DATABASE_NAME = os.getenv('DATABASE_NAME')
    DATABASE_USER = os.getenv('DATABASE_USER')
    DATABASE_PORT = os.getenv('DATABASE_PORT')
    DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')
    DATABASE_HOST = os.getenv('DATABASE_HOST')

    engine = create_engine(f'postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}')

    with engine.connect() as conn:
        resort = conn.execute(text('select 1;'))
        for i in resort:
            print(i)

    articles_df.to_sql('articles',engine , if_exists='replace',index=False )


def main():
    data=extract_articles()
    articles_df=transform_articles(data)
    load_articles(articles_df)

    print('ETL process completed successfully.')


if __name__ == "__main__":
    main()      