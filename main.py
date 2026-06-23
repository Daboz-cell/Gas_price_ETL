from extract import extract_articles
from load import load_articles
from transform import transform_articles

def main():
    data=extract_articles()
    articles_df=transform_articles(data)
    load_articles(articles_df)

    print('ETL process completed successfully.')


if __name__ == "__main__":
    main()