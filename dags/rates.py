from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# define here for simplicity, better use airflow variables
api_key = "e40f31b8ebae31760adaa8a24ba1fed7"


#  get the exchange rate from fixer.io based on Base Currency
def get_exchange_rate():
    # url = f'http://data.fixer.io/api/latest?access_key={api_key}'
    # response = requests.get(url)
    # data = response.json()
    # # get only base, date, rates
    # data = {key: data[key] for key in ['base', 'date', 'rates']}
    # return data

    # return mock data for testing
    return {'base': 'EUR', 'date': '2021-09-10', 'rates': {'USD': 1.1825, 'GBP': 0.8575, 'JPY': 130.9}}


def add_exchange_rate(ti):
    data = ti.xcom_pull(task_ids='get_rates')

    current_date = data['date']
    current_base = data['base']

    for currency, rate in data['rates'].items():
        # add to database
        insert_query = f"INSERT INTO exchange_rates (date, base_currency, target_currency, rate)  VALUES ('{current_date}', '{current_base}', '{currency}', {rate})"
        # execute query

        PostgresOperator(
            task_id=f'insert_{currency}',
            postgres_conn_id='postgres',
            sql=insert_query
        ).execute(context=ti)


with DAG(
        'rates_dag',
        start_date=datetime(2023, 1, 1),
        schedule_interval="@daily",
        catchup=False
) as dag:
    # get rates using python operator
    get_rates = PythonOperator(
        task_id='get_rates',
        python_callable=get_exchange_rate
    )

    # add rates to database using python operator
    add_rates = PythonOperator(
        task_id='add_rates',
        python_callable=add_exchange_rate
    )

    get_rates >> add_rates

if __name__ == '__main__':
    # test get_exchange rate
    print(get_exchange_rate())
