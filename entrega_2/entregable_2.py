import requests
import pandas as pd
import redshift_connector
from dotenv import dotenv_values

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

    symbol_df = pd.DataFrame(data['Weekly Time Series'])
    symbol_df = symbol_df.T
    symbol_df.reset_index(inplace=True)
    symbol_df.rename(columns={'index':'week_from', '1. open':'open', '2. high':'high', '3. low': 'low', 
                       '4. close':'close', '5. volume':'volume'}, inplace=True)
    
    symbol_df['symbol'] = data['Meta Data']['2. Symbol']

    df = pd.concat([df, symbol_df])

conn = redshift_connector.connect(
    host=env_vars['HOST'],
    port=int(env_vars['PORT']),
    database=env_vars['DATABASE'],
    user=env_vars['USER'],
    password=env_vars['PASSWORD']
 )

# Create a Cursor object
cursor = conn.cursor()

# Eliminate the table if exists
cleaner_query = '''DROP TABLE IF EXISTS laureanoengulian_coderhouse.big_five_weekly;'''
cursor.execute(cleaner_query)
conn.commit()

# Create table if not exists
create_table_query = '''CREATE TABLE IF NOT EXISTS laureanoengulian_coderhouse.big_five_weekly(
                        "week_from" date not null,
                        "open" decimal(38, 4) not null,
                        "high" decimal(38, 4) not null,
                        "low" decimal(38, 4) not null,
                        "close" decimal(38, 4) not null,
                        "volume" bigint not null,
                        "symbol" varchar(15) not null);
                     '''
cursor.execute(create_table_query)
conn.commit()

# Load the df in Redshift
cursor.write_dataframe(df=df, table='big_five_weekly')
conn.commit()




