from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import sys
import os

# Ajoute le dossier racine au path pour qu'Airflow trouve tes scripts
sys.path.append(os.path.abspath("/opt/airflow"))

from scripts.ingest_climate import ingest_climate_api
from scripts.ingest_agriculture import run_ingestion

def create_buckets():
    s3 = boto3.client("s3", 
                      endpoint_url="http://minio:9000", 
                      aws_access_key_id="minioadmin", 
                      aws_secret_access_key="minioadmin123")
    for bucket in ["bronze", "silver", "gold"]:
        existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)

# Un seul bloc "with DAG" suffit
with DAG(
    'agriclimate_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, 
    catchup=False
) as dag:

    # 1. Création des buckets
    task_setup = PythonOperator(
        task_id='setup_minio_buckets',
        python_callable=create_buckets
    )

    # 2. Ingestion Agriculture
    task_ingest_agri = PythonOperator(
        task_id='ingest_agriculture_to_bronze',
        python_callable=run_ingestion
    )

    # 3. Ingestion Climat
    task_ingest_climate = PythonOperator(
        task_id='ingest_climate_to_bronze',
        python_callable=ingest_climate_api
    )

    # Définition des dépendances
    task_setup >> [task_ingest_agri, task_ingest_climate]
