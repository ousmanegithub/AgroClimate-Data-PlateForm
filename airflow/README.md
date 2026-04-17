# Module Batch Airflow + Spark

Ce dossier contient la partie batch quotidienne de la plateforme.

## Objectif

Orchestrer un traitement batch quotidien avec Airflow sur PostgreSQL:

- Source 1: `daily_farm_performance`
- Source 2: `daily_market_prices`
- Traitement Spark: jointure + calcul KPI agri-business
- Sorties:
  - Parquet partitionne dans `airflow/data/gold/agro_daily_kpi/`
  - Table PostgreSQL `daily_agro_kpi_batch`
  - CSV de synthese dans `airflow/data/gold/`

## Structure

- `docker-compose.yml`: stack Airflow + Postgres
- `Dockerfile`: image Airflow avec Java + PySpark + driver JDBC Postgres
- `initdb/01_init_agro_tables.sql`: creation + seed des 2 tables sources
- `dags/daily_agro_batch_pipeline.py`: DAG batch quotidien
- `dags/daily_agro_data_quality.py`: DAG qualite quotidien (gate avant batch)
- `jobs/agro_batch_spark.py`: job Spark execute par le DAG
- `jobs/data_quality_checks.py`: regles de qualite et reporting

## Lancement

Depuis `airflow/`:

```bash
docker compose up -d --build
```

## Interfaces

- Airflow UI: http://localhost:8086
- Login: `admin`
- Password: `admin`

## DAG

- DAG ID: `daily_agro_batch_pipeline`
- Schedule: `0 2 * * *` (quotidien a 02:00)
- Etapes:
  1. `validate_source_tables`
  2. `run_spark_batch_job`
- Mode de chargement: **incremental load**
  - Watermark = `MAX(date_obs)` de `daily_agro_kpi_batch`
  - Le job traite uniquement les lignes source avec date > watermark
  - Pas de full refresh ni purge quotidienne

- DAG ID: `daily_agro_data_quality`
- Schedule: `30 1 * * *` (quotidien a 01:30)
- Etapes:
  1. `init_quality_table`
  2. `run_quality_gate`
- Si un controle critique echoue, le DAG est en echec (gate bloque).

## Verification donnees

Dans PostgreSQL (port hote `5433`):

- `daily_farm_performance`
- `daily_market_prices`
- `daily_agro_kpi_batch`
- `daily_data_quality_results`

Sorties fichiers:

- `airflow/data/gold/agro_daily_kpi/` (Parquet)
- `airflow/data/gold/agro_daily_summary_*.csv/` (dossier CSV Spark)

## Notes techniques

- Le DAG utilise la connexion Airflow `agro_postgres`.
- Le job Spark tourne en mode local (`local[2]`) dans le conteneur Airflow.
- Le traitement batch est incremental base sur watermark (date max deja chargee).
