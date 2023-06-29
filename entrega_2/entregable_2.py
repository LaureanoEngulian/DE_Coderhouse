import time

import pandas as pd
import psycopg2
import requests
import sqlalchemy as sa
from dotenv import dotenv_values
from sqlalchemy.engine.url import URL

env_vars = dotenv_values('.env')

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
# url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo'

function = 'TIME_SERIES_WEEKLY'
# Big Five Tech: Google, Amazon, Meta, Apple, and Microsoft (GAMAM)
symbols = ['GOOG', 'AMZN', 'METV', 'AAPL', 'MSFT']
api_key = 'WNHSBPLUX5B8HNMZ'

df = pd.DataFrame()

for symbol in symbols:
    url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&apikey={api_key}'
    r = requests.get(url)
    data = r.json()

    time.sleep(5)

    symbol_df = pd.DataFrame(data['Weekly Time Series'])
    symbol_df = symbol_df.T
    symbol_df.reset_index(inplace=True)
    symbol_df.rename(columns={'index': 'week_from', '1. open': 'open', '2. high': 'high', '3. low': 'low',
                              '4. close': 'close', '5. volume': 'volume'}, inplace=True)

    symbol_df['symbol'] = data['Meta Data']['2. Symbol']

    symbol_df['open'] = pd.to_numeric(symbol_df['open'])
    symbol_df['close'] = pd.to_numeric(symbol_df['close'])
    symbol_df['avg'] = (symbol_df['open'] + symbol_df['close'])/2
    symbol_df['pk'] = symbol_df['symbol']+symbol_df['week_from']

    # symbol_df['avg'] = symbol_df[['close', 'open']].mean(axis=1)

    df = pd.concat([df, symbol_df])

url = URL.create(
drivername='redshift+redshift_connector', # indicate redshift_connector driver and dialect will be used
host=env_vars['HOST'], # Amazon Redshift host
port=int(env_vars['PORT']), # Amazon Redshift port
database=env_vars['DATABASE'], # Amazon Redshift database
username=env_vars['USER'], # Amazon Redshift username
password=env_vars['PASSWORD'] # Amazon Redshift password
)

engine = sa.create_engine(url)

df.to_sql(name='stage',
          con=engine,
          if_exists='append',
          index=False)

# Connect to Redshift using psycopg2
conn = psycopg2.connect(
    host=env_vars['HOST'],
    port=int(env_vars['PORT']),
    database=env_vars['DATABASE'],
    user=env_vars['USER'],
    password=env_vars['PASSWORD']
)

cursor = conn.cursor()


# https://docs.aws.amazon.com/redshift/latest/dg/merge-replacing-existing-rows.html
sql_transaction = ''' begin transaction;
                      
                      CREATE TABLE IF NOT EXISTS laureanoengulian_coderhouse.big_five_weekly(
                      "week_from" varchar(256) not null,
                      "open" float8 not null,
                      "high" varchar(256) not null,
                      "low" varchar(256) not null,
                      "close" float8 not null,
                      "volume" varchar(256) not null,
                      "symbol" varchar(256) not null,
                      "avg" float8 not null,
                      "pk" varchar(256));


                      delete from laureanoengulian_coderhouse.big_five_weekly using laureanoengulian_coderhouse.stage 
                      where big_five_weekly.pk = stage.pk;

                      insert into laureanoengulian_coderhouse.big_five_weekly
                      select * from laureanoengulian_coderhouse.stage;

                      end transaction;
                  '''

cursor.execute(sql_transaction)
conn.commit()

drop_tmp = '''drop table if exists laureanoengulian_coderhouse.stage;'''

cursor.execute(drop_tmp)
conn.commit()

# Chequeo de valores únicos
cursor = conn.cursor()
cursor.execute(f"""
SELECT
  count(pk), count(distinct pk)
FROM
  laureanoengulian_coderhouse.big_five_weekly;
""")
# resultado = cursor.fetchall()
print(", ".join(map(lambda x: str(x), cursor.fetchall())))
cursor.close()
