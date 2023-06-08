import logging
import os
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow import settings
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def create_connection_if_not_exists(conn_id, conn_type, host, port, login, password):
    try:
        BaseHook.get_connection(conn_id)
    except AirflowNotFoundException:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            port=port,
        )

        session = settings.Session()
        session.add(new_conn)
        session.commit()
        session.close()
        logging.info(f"Created connection: {conn_id}")
        return None

    logging.info(f"Connection already exists: {conn_id}")


def get_connection(conn_id):
    conn = BaseHook.get_connection(conn_id)

    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password
    )


def get_exchange_rates():
    api_key = os.getenv("FIXER_API_KEY")
    url = f'http://data.fixer.io/api/latest?access_key={api_key}'
    response = requests.get(url)
    data = response.json()
    # get only base, date, rates
    data = {key: data[key] for key in ['base', 'date', 'rates']}
    return data


def transform_rates(ti):
    rates = ti.xcom_pull(task_ids='get_rates')

    base = rates['base']
    date = rates['date']
    rates = rates['rates']

    transformed_rates = []
    for currency in rates:
        transformed_rate = {
            'base': base,
            'currency': currency,
            'date': date,
            'value': rates[currency]
        }
        transformed_rates.append(transformed_rate)

    return transformed_rates


def sort_rates(ti):
    rates = ti.xcom_pull(task_ids='transform_rates')

    conn = get_connection('postgres')

    cur = conn.cursor()

    to_insert = []
    to_update = []

    for rate in rates:
        cur.execute(
            'SELECT * FROM exchange_rates WHERE date=%s AND base_currency=%s AND target_currency=%s',
            (rate['date'], rate['base'], rate['currency'])
        )

        r = cur.fetchone()

        if r is None:
            to_insert.append(rate)
        elif float(r[4]) != rate['value']:
            logging.info(f"rate is different: {r[4]} != {rate['value']}")
            to_update.append(rate)

    cur.close()
    conn.close()

    return {'to_insert': to_insert, 'to_update': to_update}


def insert_rates(ti):
    rates = ti.xcom_pull(task_ids='sort_rates')['to_insert']

    if len(rates) == 0:
        return

    conn = get_connection('postgres')

    cur = conn.cursor()

    for rate in rates:
        cur.execute(
            'INSERT INTO exchange_rates (date, base_currency, target_currency, value) VALUES (%s, %s, %s, %s)',
            (rate['date'], rate['base'], rate['currency'], rate['value'])
        )

    conn.commit()
    cur.close()
    conn.close()


def update_rates(ti):
    rates = ti.xcom_pull(task_ids='sort_rates')['to_update']

    if len(rates) == 0:
        return

    conn = get_connection('postgres')

    cur = conn.cursor()

    for rate in rates:
        cur.execute(
            'UPDATE exchange_rates SET value=%s WHERE date=%s AND base_currency=%s AND target_currency=%s',
            (rate['value'], rate['date'], rate['base'], rate['currency'])
        )

    conn.commit()
    cur.close()
    conn.close()


with DAG(
        'rates_dag',
        start_date=datetime(2023, 1, 1),
        schedule_interval="@daily",
        catchup=False
) as dag:
    # TODO : handle when variables are not set, based on requirements
    db_connection_id = os.getenv('CONNECTION_ID')
    db_connection_type = os.getenv('CONNECTION_TYPE')
    db_host = os.getenv('HOST')
    db_port = os.getenv('PORT')
    db_login = os.getenv('LOGIN')
    db_password = os.getenv('PASSWORD')
    db_database = os.getenv('DATABASE')

    # create connection to database if not exists
    provide_postgres = PythonOperator(
        task_id='provide_postgres',
        python_callable=create_connection_if_not_exists,
        op_kwargs={
            'conn_id': db_connection_id,
            'conn_type': db_connection_type,
            'host': db_host,
            'port': db_port,
            'login': db_login,
            'password': db_password,
        },
        dag=dag
    )

    # create table if not exists
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=db_connection_id,
        # better use sql file, but for simplicity here
        sql="""
        CREATE TABLE IF NOT EXISTS exchange_rates
        (
            id              SERIAL PRIMARY KEY,
            date            DATE       NOT NULL,
            base_currency   VARCHAR(3) NOT NULL,
            target_currency VARCHAR(3) NOT NULL,
            value            DECIMAL    NOT NULL
        );
        """,
        dag=dag
    )

    # get exchange rates from fixer.io
    get_rates = PythonOperator(
        task_id='get_rates',
        python_callable=get_exchange_rates,
        dag=dag
    )

    # transform data into a list of rates
    transform_rates = PythonOperator(
        task_id='transform_rates',
        python_callable=transform_rates,
        dag=dag
    )

    # sort rates into two lists, one for insert, one for update
    sort_rates = PythonOperator(
        task_id='sort_rates',
        python_callable=sort_rates,
        dag=dag
    )

    # insert rates into database
    insert_rates = PythonOperator(
        task_id='insert_rates',
        python_callable=insert_rates,
        dag=dag
    )

    # update rates in database
    update_rates = PythonOperator(
        task_id='update_rates',
        python_callable=update_rates,
        dag=dag
    )

    provide_postgres >> create_table >> get_rates >> transform_rates >> sort_rates >> [insert_rates, update_rates]
