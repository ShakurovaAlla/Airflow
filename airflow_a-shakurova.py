import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


games_data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'
year = 1994 + hash(f'a-shakurova') % 23

default_args = {
    'owner': 'a-shakurova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 05),
    'schedule_interval': '* 10 * * *'
}

@dag(default_args=default_args, catchup=False)
def top_games():

    # Загрузка данных
    @task()
    def get_data():
        df = pd.read_csv(games_data)
        df_sales = df.query('Year==@year')
        return df_sales

    # Какая игра была самой продаваемой в этом году во всем мире?
    @task()
    def top_game_global(df_sales):
        top_global = sales.groupby('Name', as_index=False)['Global_Sales'].sum() \
                          .sort_values('Global_Sales', ascending=False).iloc[0,0]
        return top_global 
    
    # Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько.
    @task()
    def top_genre_EU(df_sales):
        top_eu = sales.groupby('Genre', as_index=False)['EU_Sales']
                      .sum().sort_values('EU_Sales', ascending=False)
        max_eu = top_eu.EU_Sales.max()
        max_EU = top_eu.query('EU_Sales==@max_eu_sales')['Genre'].tolist()
        return max_EU 

    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
      Перечислить все, если их несколько. 
    @task()
    def top_name_NA(df_sales):
        top_na = sales.query('NA_Sales > 1').groupby('Platform', as_index=False)['Name'] \
                                           .count().sort_values('Name', ascending=False)
	top_na_max = top_na.Name.max()
	top_NA = top_na.query('Name==@top_na_max')['Platform'].to_list()
        return top_NA
    
    # У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task()
    def top_JP_Publisher(df_sales):
        sales_jp = sales.groupby('Publisher', as_index=False)['JP_Sales'].mean() \
                        .sort_values('JP_Sales', ascending=False)
	sales_jp_max = sales_jp.JP_Sales.max()
	sales_JP = sales_jp.query('JP_Sales==@sales_jp_max')['Publisher'].to_list()
        return sales_JP 
    
    # Сколько игр продались лучше в Европе, чем в Японии?
    @task()
    def jp_eu_game(df_sales):
        game_eu_jp = sales.groupby('Name', as_index=False).agg({'EU_Sales': 'sum', 'JP_Sales': 'sum'})
        game_EU_JP = game_eu_jp.query('EU_Sales > JP_Sales').shape[0]
        return game_EU_JP 

    @task()
    def print_data(top_game_global, top_genre_EU, top_name_NA, top_JP_Publisher, jp_eu_game):

        context = get_current_context()
        date = context['ds']
  
        print(f'For {date}. The best selling game in {year}')
        print(top_game_global)

        print(f'For {date}. The best selling game genre in Europe in {year}')
        print(top_genre_EU)

        print(f'For {date}. The best selling game in NA with more than 1 million sales in {year}')
        print(top_name_NA)

        print(f'For {date}. The best selling game Publisher in Japan in {year}')
        print(top_JP_Publisher)

        print(f'For {date}. The number of games sold in Europe is more than in Japan in {year}')
        print(jp_eu_game)

    df_sales = get_data()
    top_game_global = top_game_global(df_sales)
    top_genre_EU = top_genre_EU(df_sales)
    top_name_NA = top_name_NA(df_sales)
    top_JP_Publisher = top_JP_Publisher(df_sales)
    jp_eu_game = jp_eu_game(df_sales)

    print_data(top_game_global, top_genre_EU, top_name_NA, top_JP_Publisher, jp_eu_game)

top_games = top_games()
