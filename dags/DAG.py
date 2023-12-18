from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from Scripts.etl import extract_data, transform_data, load_data
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import requests
import io
import time
import logging
from sqlalchemy import create_engine

import sys
from dotenv import load_dotenv
import os
import warnings

sys.path.append(os.path.join(os.environ['AIRFLOW_HOME'], 'dags/Scripts'))


from etl import etl_process


warnings.filterwarnings("ignore", category=DeprecationWarning) 


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'nba_data_etl',
    default_args=default_args,
    description='ETL DAG for NBA Data',
    schedule_interval=timedelta(days=1),
    catchup=True
)

etl_task = PythonOperator(
    task_id='nba_etl',
    python_callable=etl_process,
    dag=dag
)

etl_task
