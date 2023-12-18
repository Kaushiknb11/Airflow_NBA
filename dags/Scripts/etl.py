from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import requests
import io
import time
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from bs4 import BeautifulSoup


# Load environment variables
load_dotenv("./dags/secrets.env")

# Database connection details
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
YOUR_ACCESS_KEY = os.getenv('YOUR_ACCESS_KEY')
YOUR_SECRET_KEY = os.getenv('YOUR_SECRET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def extract_data():
    url = "https://www.basketball-reference.com/leagues/NBA_2024_per_game.html"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"id": "per_game_stats"})

    headers = ["Player", "Pos", "Age", "Tm", "G", "GS", "MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%",
               "2P", "2PA", "2P%", "eFG%", "FT", "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK",
               "TOV", "PF", "PTS"]

    rows = []
    for row in table.find_all("tr"):
        data = [cell.get_text() for cell in row.find_all("td")]
        if data:
            rows.append(data)

    df = pd.DataFrame(rows, columns=headers)
    return df

def transform_data(df):
    # Placeholder for data transformation - currently, there's no transformation needed.
    pass

def load_data(df):
    engine = create_engine(DATABASE_URL)
    df.to_sql('nba_data', engine, if_exists='replace', index=False)

def etl_process():
    df = extract_data()
    transform_data(df)
    load_data(df)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting ETL process")
    etl_process()
    logging.info("ETL process finished successfully")
