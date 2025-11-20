'''
=================================================

This script is to do automation tranform and load data from PostgreSQL to ElasticSearch. Dataset used is data of Global Renewable Energy Usage.

=================================================
'''

# Import Libraries

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

# Functions 

# Fetch Data from PostgreSQL
def fetch_from_postgres():
    '''
    This function is to load or fetch data from PostgreSQL and save the data to csv file as raw data.
    '''
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'" #access to PostgreSQL
    conn = db.connect(conn_string)

    query = "SELECT * FROM table_m3"
    df = pd.read_sql(query, conn)
    df.to_csv('/opt/airflow/dags/data_raw.csv', index=False) #save as csv file

    conn.close() #to close connection

# Data Cleaning
def data_cleaning():
    '''
    This function is to clean the dataset and then save as clean data. There are two step of data cleaning, drop duplicates and handle missing value.
    '''
    df = pd.read_csv('/opt/airflow/dags/data_raw.csv') #load data

    # 1. Drop duplicates
    df.drop_duplicates(inplace=True) #drop duplicated data, and make every data unique

    # 2. Handle missing values
    # - Categorical = 'unknown', assume str unknown as category that not in in the class.
    # - Numerical = 0, all the numerical value in dataset are not/cannot be zero, assume zero is value of missing value.
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col].fillna('unknown', inplace=True)
        else:
            df[col].fillna(0, inplace=True)

    # Save cleaned data
    df.to_csv('/opt/airflow/dags/data_clean.csv', index=False) #save clean data as csv file

# Add to ElasticSearch
def to_elasticsearch():
    '''
    This function is to change data to json and add the data to ElasticSearch.
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/data_clean.csv') #load clean data

    for i,r in df.iterrows():
        doc=r.to_json()
        es.index(index="projectm3", doc_type="doc", body=doc, id=i)

# DAG Setup

default_args = {
    'owner': 'ilham',
    'start_date': dt.datetime(2024, 11, 1, 9, 10, 0) - timedelta(hours=8), #run time strat at 01 November 2024 at 9.10am
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('PROJECTDAG',
         default_args=default_args,
         schedule_interval= '10-30/10 9 * * 6', #the run will be every saturday every 10min from 9.10am until after 9.30am(last run on that day)
         catchup=False #skip the run between start date and present run
         ) as dag:

    # Tasks (call all functions)
    
    task_fetch = PythonOperator(
        task_id='fetch_from_postgres',
        python_callable=fetch_from_postgres,
    )

    task_clean = PythonOperator(
        task_id='data_cleaning',
        python_callable=data_cleaning,
    )

    task_es = PythonOperator(
        task_id='to_elasticsearch',
        python_callable=to_elasticsearch,
    )

task_fetch >> task_clean >> task_es #run sequence