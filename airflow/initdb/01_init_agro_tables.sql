CREATE TABLE IF NOT EXISTS daily_farm_performance (
    id SERIAL PRIMARY KEY,
    parcelle_id VARCHAR(50) NOT NULL,
    region VARCHAR(100) NOT NULL,
    culture VARCHAR(50) NOT NULL,
    date_obs DATE NOT NULL,
    surface_ha NUMERIC(10,2),
    rendement_estime_kg_ha NUMERIC(10,2),
    production_estimee_tonnes NUMERIC(10,2),
    biomasse_estimee NUMERIC(10,2),
    besoin_eau_estime_mm NUMERIC(10,2),
    stress_hydrique_score NUMERIC(5,2),
    fertilite_sol_score NUMERIC(5,2),
    risque_maladie_score NUMERIC(5,2),
    score_sante_culture NUMERIC(5,2),
    cout_journalier_fcfa NUMERIC(12,2),
    revenu_estime_fcfa NUMERIC(12,2),
    statut_culture VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS daily_market_prices (
    id SERIAL PRIMARY KEY,
    region VARCHAR(100) NOT NULL,
    culture VARCHAR(50) NOT NULL,
    date_prix DATE NOT NULL,
    prix_kg_fcfa NUMERIC(10,2),
    prix_tonne_fcfa NUMERIC(12,2),
    demande_locale NUMERIC(10,2),
    offre_disponible_tonnes NUMERIC(10,2),
    indice_penurie NUMERIC(5,2),
    indice_volatilite_prix NUMERIC(5,2),
    cout_transport_fcfa NUMERIC(10,2),
    revenu_potentiel_fcfa NUMERIC(12,2),
    niveau_risque_marche VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM daily_farm_performance LIMIT 1) THEN
    INSERT INTO daily_farm_performance (
      parcelle_id, region, culture, date_obs, surface_ha, rendement_estime_kg_ha,
      production_estimee_tonnes, biomasse_estimee, besoin_eau_estime_mm,
      stress_hydrique_score, fertilite_sol_score, risque_maladie_score,
      score_sante_culture, cout_journalier_fcfa, revenu_estime_fcfa, statut_culture
    )
    SELECT
      'PARC-' || (1000 + row_number() OVER())::TEXT,
      r.region,
      c.culture,
      d.date_obs,
      ROUND((1.0 + random() * 9.0)::numeric, 2),
      ROUND((1500 + random() * 3500)::numeric, 2),
      ROUND((2 + random() * 12)::numeric, 2),
      ROUND((1 + random() * 8)::numeric, 2),
      ROUND((3 + random() * 18)::numeric, 2),
      ROUND((random() * 5)::numeric, 2),
      ROUND((4 + random() * 6)::numeric, 2),
      ROUND((random() * 5)::numeric, 2),
      ROUND((4 + random() * 6)::numeric, 2),
      ROUND((15000 + random() * 65000)::numeric, 2),
      ROUND((50000 + random() * 220000)::numeric, 2),
      CASE
        WHEN random() < 0.15 THEN 'critique'
        WHEN random() < 0.45 THEN 'stress'
        ELSE 'normal'
      END
    FROM
      (VALUES ('Dakar'), ('Diourbel'), ('Fatick'), ('Kaolack'), ('Kolda'), ('Saint-Louis'), ('Thies'), ('Ziguinchor')) AS r(region)
    CROSS JOIN
      (VALUES ('riz'), ('mais'), ('arachide'), ('mil'), ('tomate')) AS c(culture)
    CROSS JOIN
      (SELECT generate_series(CURRENT_DATE - INTERVAL '20 day', CURRENT_DATE, INTERVAL '1 day')::date AS date_obs) d;
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM daily_market_prices LIMIT 1) THEN
    INSERT INTO daily_market_prices (
      region, culture, date_prix, prix_kg_fcfa, prix_tonne_fcfa,
      demande_locale, offre_disponible_tonnes, indice_penurie,
      indice_volatilite_prix, cout_transport_fcfa, revenu_potentiel_fcfa,
      niveau_risque_marche
    )
    SELECT
      r.region,
      c.culture,
      d.date_prix,
      ROUND((120 + random() * 680)::numeric, 2),
      ROUND((120000 + random() * 680000)::numeric, 2),
      ROUND((80 + random() * 500)::numeric, 2),
      ROUND((30 + random() * 260)::numeric, 2),
      ROUND((random() * 5)::numeric, 2),
      ROUND((random() * 5)::numeric, 2),
      ROUND((10000 + random() * 45000)::numeric, 2),
      ROUND((60000 + random() * 260000)::numeric, 2),
      CASE
        WHEN random() < 0.2 THEN 'eleve'
        WHEN random() < 0.6 THEN 'moyen'
        ELSE 'faible'
      END
    FROM
      (VALUES ('Dakar'), ('Diourbel'), ('Fatick'), ('Kaolack'), ('Kolda'), ('Saint-Louis'), ('Thies'), ('Ziguinchor')) AS r(region)
    CROSS JOIN
      (VALUES ('riz'), ('mais'), ('arachide'), ('mil'), ('tomate')) AS c(culture)
    CROSS JOIN
      (SELECT generate_series(CURRENT_DATE - INTERVAL '20 day', CURRENT_DATE, INTERVAL '1 day')::date AS date_prix) d;
  END IF;
END $$;
