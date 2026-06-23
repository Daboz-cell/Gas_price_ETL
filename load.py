from sqlalchemy import create_engine , text
import os
from dotenv import load_dotenv

load_dotenv()


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