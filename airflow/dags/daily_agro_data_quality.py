from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from jobs.data_quality_checks import ensure_quality_table, run_data_quality_checks


with DAG(
    dag_id="daily_agro_data_quality",
    start_date=datetime(2026, 4, 1),
    schedule="30 1 * * *",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["quality", "postgres", "agro"],
    description="Controle qualite quotidien des donnees farm et market avant batch Spark",
) as dag:
    init_quality_table = PythonOperator(
        task_id="init_quality_table",
        python_callable=ensure_quality_table,
    )

    run_quality_gate = PythonOperator(
        task_id="run_quality_gate",
        python_callable=run_data_quality_checks,
        op_kwargs={"fail_on_error": True},
    )

    init_quality_table >> run_quality_gate
