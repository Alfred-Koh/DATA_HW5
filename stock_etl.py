from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests

ALPHA_VINTAGE_API_KEY = Variable.get("alpha_vintage_api_key")
STOCK_TICKER = Variable.get("stock_ticker")
SNOWFLAKE_TABLE = Variable.get("snowflake_table")


def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(url):
    res = requests.get(url)
    return res.json()


@task
def transform(data):
    results = []
    for date in data["Time Series (Daily)"]:
        item = data['Time Series (Daily)'][date]
        item['date'] = date
        results.append(item)

    return results[:90]


@task
def load(data):
    conn = get_snowflake_conn()
    try:
        conn.execute('BEGIN;')
        conn.execute(f"""
          CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE}(
            symbol STRING,
            date DATE,
            open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume INT,
            PRIMARY KEY (symbol, date)
          );
        """)
        conn.execute(f'DELETE FROM {SNOWFLAKE_TABLE};')

        for day in data:
            date = day['date']
            open = day["1. open"]
            high = day["2. high"]
            low = day["3. low"]
            close = day["4. close"]
            volume = day["5. volume"]
            conn.execute(
                f"INSERT INTO {SNOWFLAKE_TABLE} (symbol, date, open, high, low, close, volume) VALUES ('{STOCK_TICKER}', '{date}', {open}, {high}, {low}, {close}, {volume})")

        conn.execute('COMMIT;')
    except Exception as e:
        conn.execute('ROLLBACK;')
        print(e)
        raise e


with DAG(
    dag_id='stock_etl',
    schedule_interval='0 1 * * *',
    start_date=datetime(2025, 10, 4),
    catchup=False,
    tags=['ETL']
) as dag:
    conn = get_snowflake_conn()
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={STOCK_TICKER}&apikey={ALPHA_VINTAGE_API_KEY}"
    raw_data = extract(url)
    transformed_data = transform(raw_data)
    load(transformed_data)
