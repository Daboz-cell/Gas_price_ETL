import pandas as pd

def transform_articles(data):
    articles_list=data['articles']

    articles_df=pd.DataFrame(articles_list)

    articles_df.drop(columns=['source','urlToImage'],inplace=True)

    articles_df.rename(columns={'publishedAt':'published_at'},inplace=True)

    return articles_df