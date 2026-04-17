from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _get_last_processed_date() -> Optional[str]:
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "agrodb")
    postgres_user = os.getenv("POSTGRES_USER", "agro")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "agro")

    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date_obs) FROM daily_agro_kpi_batch;")
            value = cur.fetchone()[0]
            return value.strftime("%Y-%m-%d") if value else None
    finally:
        conn.close()


def run_batch_job() -> None:
    postgres_host = os.getenv("POSTGRES_HOST", "postgres")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "agrodb")
    postgres_user = os.getenv("POSTGRES_USER", "agro")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "agro")

    jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    jdbc_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }

    spark = (
        SparkSession.builder.appName("agro-batch-daily")
        .master("local[2]")
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar")
        .getOrCreate()
    )

    last_processed_date = _get_last_processed_date()

    if last_processed_date:
        farm_query = f"(SELECT * FROM daily_farm_performance WHERE date_obs > DATE '{last_processed_date}') farm_incr"
        market_query = f"(SELECT * FROM daily_market_prices WHERE date_prix > DATE '{last_processed_date}') market_incr"
        print(f"[batch] Incremental mode enabled from watermark > {last_processed_date}")
    else:
        farm_query = "daily_farm_performance"
        market_query = "daily_market_prices"
        print("[batch] First load (no watermark). Historical load in progress.")

    farm_df = spark.read.jdbc(url=jdbc_url, table=farm_query, properties=jdbc_properties)
    market_df = spark.read.jdbc(url=jdbc_url, table=market_query, properties=jdbc_properties)

    if farm_df.rdd.isEmpty():
        print("[batch] No new farm rows to process. Job completed.")
        spark.stop()
        return

    joined_df = farm_df.alias("f").join(
        market_df.alias("m"),
        on=[
            F.col("f.region") == F.col("m.region"),
            F.col("f.culture") == F.col("m.culture"),
            F.col("f.date_obs") == F.col("m.date_prix"),
        ],
        how="inner",
    )

    if joined_df.rdd.isEmpty():
        print("[batch] No matching farm/market rows in incremental window. Job completed.")
        spark.stop()
        return

    kpi_df = (
        joined_df.select(
            F.col("f.date_obs").alias("date_obs"),
            F.col("f.region").alias("region"),
            F.col("f.culture").alias("culture"),
            F.col("f.parcelle_id").alias("parcelle_id"),
            F.col("f.production_estimee_tonnes").cast("double").alias("production_estimee_tonnes"),
            F.col("f.revenu_estime_fcfa").cast("double").alias("revenu_estime_fcfa"),
            F.col("m.revenu_potentiel_fcfa").cast("double").alias("revenu_potentiel_fcfa"),
            (
                F.col("f.revenu_estime_fcfa").cast("double")
                + F.col("m.revenu_potentiel_fcfa").cast("double")
                - F.col("f.cout_journalier_fcfa").cast("double")
                - F.col("m.cout_transport_fcfa").cast("double")
            ).alias("marge_estimee_fcfa"),
            (
                F.col("f.risque_maladie_score").cast("double") * F.lit(0.4)
                + F.col("f.stress_hydrique_score").cast("double") * F.lit(0.3)
                + F.col("m.indice_volatilite_prix").cast("double") * F.lit(0.3)
            ).alias("risque_global_score"),
            F.col("f.score_sante_culture").cast("double").alias("score_sante_culture"),
            F.col("m.indice_penurie").cast("double").alias("indice_penurie"),
        )
        .withColumn(
            "priorite_intervention",
            F.when((F.col("risque_global_score") >= 3.5) | (F.col("marge_estimee_fcfa") < 0), F.lit("critique"))
            .when(F.col("risque_global_score") >= 2.3, F.lit("surveillance"))
            .otherwise(F.lit("stable")),
        )
        .withColumn("created_at", F.current_timestamp())
    )

    output_base = "/opt/airflow/data/gold/agro_daily_kpi"
    (kpi_df.write.mode("append").partitionBy("date_obs").parquet(output_base))

    (
        kpi_df.select(
            "date_obs",
            "region",
            "culture",
            "parcelle_id",
            F.round("production_estimee_tonnes", 2).alias("production_estimee_tonnes"),
            F.round("revenu_estime_fcfa", 2).alias("revenu_estime_fcfa"),
            F.round("revenu_potentiel_fcfa", 2).alias("revenu_potentiel_fcfa"),
            F.round("marge_estimee_fcfa", 2).alias("marge_estimee_fcfa"),
            F.round("risque_global_score", 2).alias("risque_global_score"),
            F.round("score_sante_culture", 2).alias("score_sante_culture"),
            F.round("indice_penurie", 2).alias("indice_penurie"),
            "priorite_intervention",
            "created_at",
        )
        .write.mode("append")
        .jdbc(url=jdbc_url, table="daily_agro_kpi_batch", properties=jdbc_properties)
    )

    summary_df = (
        kpi_df.groupBy("date_obs", "region", "culture", "priorite_intervention")
        .agg(
            F.count("*").alias("nb_parcelles"),
            F.round(F.avg("marge_estimee_fcfa"), 2).alias("marge_moyenne_fcfa"),
            F.round(F.avg("risque_global_score"), 2).alias("risque_moyen"),
        )
        .orderBy(F.col("date_obs").desc(), F.col("region"), F.col("culture"))
    )

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    summary_path = f"/opt/airflow/data/gold/agro_daily_summary_{timestamp}.csv"
    summary_df.coalesce(1).write.mode("overwrite").option("header", True).csv(summary_path)

    spark.stop()


if __name__ == "__main__":
    run_batch_job()
