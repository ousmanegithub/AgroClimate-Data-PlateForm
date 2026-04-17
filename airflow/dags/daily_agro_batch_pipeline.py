from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from jobs.agro_batch_spark import run_batch_job


def check_source_tables() -> None:
    hook = PostgresHook(postgres_conn_id="agro_postgres")
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
      AND table_name IN ('daily_farm_performance', 'daily_market_prices')
    ORDER BY table_name;
    """
    rows = hook.get_records(sql)
    if len(rows) != 2:
        raise ValueError("Source tables missing: daily_farm_performance and/or daily_market_prices")


def ensure_batch_targets() -> None:
    hook = PostgresHook(postgres_conn_id="agro_postgres")
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS daily_agro_kpi_batch (
            id SERIAL PRIMARY KEY,
            date_obs DATE,
            region VARCHAR(100),
            culture VARCHAR(50),
            parcelle_id VARCHAR(50),
            production_estimee_tonnes NUMERIC(12,2),
            revenu_estime_fcfa NUMERIC(14,2),
            revenu_potentiel_fcfa NUMERIC(14,2),
            marge_estimee_fcfa NUMERIC(14,2),
            risque_global_score NUMERIC(8,2),
            score_sante_culture NUMERIC(8,2),
            indice_penurie NUMERIC(8,2),
            priorite_intervention VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_agro_kpi_batch_business
        ON daily_agro_kpi_batch (date_obs, region, culture, parcelle_id);
        """
    )


def profile_incremental_window(ti: Any) -> None:
    hook = PostgresHook(postgres_conn_id="agro_postgres")

    last_processed = hook.get_first("SELECT MAX(date_obs) FROM daily_agro_kpi_batch;")[0]
    if last_processed is None:
        farm_candidates = int(hook.get_first("SELECT COUNT(*) FROM daily_farm_performance;")[0])
        market_candidates = int(hook.get_first("SELECT COUNT(*) FROM daily_market_prices;")[0])
        watermark = "NULL"
    else:
        farm_candidates = int(
            hook.get_first(
                "SELECT COUNT(*) FROM daily_farm_performance WHERE date_obs > %s;",
                parameters=(last_processed,),
            )[0]
        )
        market_candidates = int(
            hook.get_first(
                "SELECT COUNT(*) FROM daily_market_prices WHERE date_prix > %s;",
                parameters=(last_processed,),
            )[0]
        )
        watermark = str(last_processed)

    ti.xcom_push(key="watermark", value=watermark)
    ti.xcom_push(key="farm_candidates", value=farm_candidates)
    ti.xcom_push(key="market_candidates", value=market_candidates)

    print(f"[batch-profile] watermark={watermark}")
    print(f"[batch-profile] farm_candidates={farm_candidates}")
    print(f"[batch-profile] market_candidates={market_candidates}")


def capture_target_rowcount_before(ti: Any) -> None:
    hook = PostgresHook(postgres_conn_id="agro_postgres")
    before_count = int(hook.get_first("SELECT COUNT(*) FROM daily_agro_kpi_batch;")[0])
    ti.xcom_push(key="target_rowcount_before", value=before_count)
    print(f"[batch-check] target rows before load: {before_count}")


def validate_post_load(ti: Any) -> None:
    hook = PostgresHook(postgres_conn_id="agro_postgres")
    before_count = int(ti.xcom_pull(task_ids="capture_target_rowcount_before", key="target_rowcount_before") or 0)
    farm_candidates = int(ti.xcom_pull(task_ids="profile_incremental_window", key="farm_candidates") or 0)
    market_candidates = int(ti.xcom_pull(task_ids="profile_incremental_window", key="market_candidates") or 0)

    after_count = int(hook.get_first("SELECT COUNT(*) FROM daily_agro_kpi_batch;")[0])
    today_inserted = int(
        hook.get_first("SELECT COUNT(*) FROM daily_agro_kpi_batch WHERE created_at::date = CURRENT_DATE;")[0]
    )

    if after_count < before_count:
        raise ValueError("Post-load validation failed: target row count decreased unexpectedly.")

    print(f"[batch-check] target rows after load: {after_count}")
    print(f"[batch-check] inserted today: {today_inserted}")

    if farm_candidates > 0 and market_candidates > 0 and after_count == before_count:
        print(
            "[batch-check] warning: candidates exist but no new rows inserted "
            "(possible join mismatch or duplicate business keys)."
        )


with DAG(
    dag_id="daily_agro_batch_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["batch", "spark", "postgres", "agro"],
    description="Traitement batch quotidien des performances agricoles et prix de marche via Spark",
) as dag:
    validate_sources = PythonOperator(
        task_id="validate_source_tables",
        python_callable=check_source_tables,
    )

    prepare_targets = PythonOperator(
        task_id="ensure_batch_targets",
        python_callable=ensure_batch_targets,
    )

    profile_window = PythonOperator(
        task_id="profile_incremental_window",
        python_callable=profile_incremental_window,
    )

    capture_before = PythonOperator(
        task_id="capture_target_rowcount_before",
        python_callable=capture_target_rowcount_before,
    )

    run_spark_batch = PythonOperator(
        task_id="run_spark_batch_job",
        python_callable=run_batch_job,
    )

    validate_after = PythonOperator(
        task_id="validate_post_load",
        python_callable=validate_post_load,
    )

    validate_sources >> prepare_targets >> profile_window >> capture_before >> run_spark_batch >> validate_after
