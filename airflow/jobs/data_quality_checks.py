from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook


@dataclass
class QualityCheck:
    name: str
    severity: str
    sql: str
    details: str


def ensure_quality_table(postgres_conn_id: str = "agro_postgres") -> None:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    hook.run(
        """
        CREATE TABLE IF NOT EXISTS daily_data_quality_results (
            id SERIAL PRIMARY KEY,
            run_id VARCHAR(64) NOT NULL,
            check_name VARCHAR(200) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            status VARCHAR(20) NOT NULL,
            failed_rows INT NOT NULL,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def run_data_quality_checks(
    postgres_conn_id: str = "agro_postgres",
    fail_on_error: bool = True,
) -> None:
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    checks = [
        QualityCheck(
            name="farm_null_critical_fields",
            severity="error",
            sql="""
                SELECT COUNT(*)
                FROM daily_farm_performance
                WHERE parcelle_id IS NULL
                   OR region IS NULL
                   OR culture IS NULL
                   OR date_obs IS NULL;
            """,
            details="Champs critiques vides dans daily_farm_performance",
        ),
        QualityCheck(
            name="market_null_critical_fields",
            severity="error",
            sql="""
                SELECT COUNT(*)
                FROM daily_market_prices
                WHERE region IS NULL
                   OR culture IS NULL
                   OR date_prix IS NULL;
            """,
            details="Champs critiques vides dans daily_market_prices",
        ),
        QualityCheck(
            name="farm_duplicate_business_key",
            severity="error",
            sql="""
                SELECT COUNT(*)
                FROM (
                    SELECT parcelle_id, date_obs, COUNT(*) AS c
                    FROM daily_farm_performance
                    GROUP BY parcelle_id, date_obs
                    HAVING COUNT(*) > 1
                ) t;
            """,
            details="Doublons sur la cle metier parcelle_id + date_obs",
        ),
        QualityCheck(
            name="market_duplicate_business_key",
            severity="error",
            sql="""
                SELECT COUNT(*)
                FROM (
                    SELECT region, culture, date_prix, COUNT(*) AS c
                    FROM daily_market_prices
                    GROUP BY region, culture, date_prix
                    HAVING COUNT(*) > 1
                ) t;
            """,
            details="Doublons sur la cle metier region + culture + date_prix",
        ),
        QualityCheck(
            name="farm_negative_values",
            severity="error",
            sql="""
                SELECT COUNT(*)
                FROM daily_farm_performance
                WHERE surface_ha < 0
                   OR rendement_estime_kg_ha < 0
                   OR production_estimee_tonnes < 0
                   OR biomasse_estimee < 0
                   OR besoin_eau_estime_mm < 0
                   OR cout_journalier_fcfa < 0
                   OR revenu_estime_fcfa < 0;
            """,
            details="Valeurs negatives interdites cote farm",
        ),
        QualityCheck(
            name="market_negative_values",
            severity="error",
            sql="""
                SELECT COUNT(*)
                FROM daily_market_prices
                WHERE prix_kg_fcfa < 0
                   OR prix_tonne_fcfa < 0
                   OR demande_locale < 0
                   OR offre_disponible_tonnes < 0
                   OR cout_transport_fcfa < 0
                   OR revenu_potentiel_fcfa < 0;
            """,
            details="Valeurs negatives interdites cote marche",
        ),
        QualityCheck(
            name="farm_scores_out_of_range",
            severity="warning",
            sql="""
                SELECT COUNT(*)
                FROM daily_farm_performance
                WHERE stress_hydrique_score NOT BETWEEN 0 AND 5
                   OR fertilite_sol_score NOT BETWEEN 0 AND 10
                   OR risque_maladie_score NOT BETWEEN 0 AND 5
                   OR score_sante_culture NOT BETWEEN 0 AND 10;
            """,
            details="Scores hors bornes metier attendues",
        ),
        QualityCheck(
            name="market_indices_out_of_range",
            severity="warning",
            sql="""
                SELECT COUNT(*)
                FROM daily_market_prices
                WHERE indice_penurie NOT BETWEEN 0 AND 5
                   OR indice_volatilite_prix NOT BETWEEN 0 AND 5;
            """,
            details="Indices marche hors bornes",
        ),
        QualityCheck(
            name="price_kg_tonne_inconsistency",
            severity="warning",
            sql="""
                SELECT COUNT(*)
                FROM daily_market_prices
                WHERE prix_kg_fcfa IS NOT NULL
                  AND prix_tonne_fcfa IS NOT NULL
                  AND ABS(prix_tonne_fcfa - (prix_kg_fcfa * 1000)) > 5000;
            """,
            details="Incoherence entre prix_kg et prix_tonne",
        ),
        QualityCheck(
            name="farm_market_join_coverage_yesterday",
            severity="warning",
            sql="""
                SELECT COUNT(*)
                FROM daily_farm_performance f
                LEFT JOIN daily_market_prices m
                  ON f.region = m.region
                 AND f.culture = m.culture
                 AND f.date_obs = m.date_prix
                WHERE f.date_obs = CURRENT_DATE - INTERVAL '1 day'
                  AND m.id IS NULL;
            """,
            details="Lignes farm non jointes au marche (J-1)",
        ),
    ]

    failed_error_checks = []

    for check in checks:
        failed_rows = int(hook.get_first(check.sql)[0])
        status = "PASS" if failed_rows == 0 else "FAIL"

        hook.run(
            """
            INSERT INTO daily_data_quality_results (run_id, check_name, severity, status, failed_rows, details)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            parameters=(run_id, check.name, check.severity, status, failed_rows, check.details),
        )

        if check.severity == "error" and failed_rows > 0:
            failed_error_checks.append((check.name, failed_rows))

    if fail_on_error and failed_error_checks:
        summary = ", ".join([f"{name}={count}" for name, count in failed_error_checks])
        raise ValueError(f"Data quality gate failed. Critical checks: {summary}")
