import marimo

__generated_with = "0.8.22"
app = marimo.App()


@app.cell
def __():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Initialisation de l’environnement Spark

        Dans cette cellule, nous configurons une session Spark dédiée au traitement distribué des données agricoles.

        ### Choix techniques

        - `SparkSession` : moteur de traitement distribué adapté aux volumes importants.
        - `local[*]` : exécution locale utilisant tous les cœurs CPU disponibles.
        - Mémoire driver configurée à 2GB pour assurer la stabilité du traitement.
        - Configuration `s3a` : permet à Spark d’interagir directement avec MinIO (compatible S3).

        ### Pourquoi S3A ?

        Le connecteur `s3a` permet :
        - Lecture/écriture directe vers MinIO
        - Stockage distribué
        - Scalabilité
        - Compatibilité cloud-native

        ### Justification métier

        L’utilisation d’un stockage objet (MinIO) simule un environnement cloud réel (type AWS S3), ce qui est cohérent avec une architecture Data Lake moderne utilisée en production.

        Cela garantit :
        - Séparation stockage / calcul
        - Traçabilité
        - Rejouabilité des pipelines
        """
    )
    return


@app.cell
def _():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, lit, abs, when, avg
    import os

    spark = (
        SparkSession.builder
        .appName("AgroBronze")
        .master("local[*]")
        .config("spark.driver.memory", "2g")


        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )

    spark.version
    return SparkSession, abs, avg, col, lit, os, spark, when


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Configuration de la connexion PostgreSQL

        Cette cellule prépare la connexion JDBC vers la base relationnelle PostgreSQL.

        ### Rôle technique

        - `postgres_url` : point d’accès à la base OLTP.
        - `properties` : paramètres d’authentification et driver JDBC.

        ### Pourquoi JDBC ?

        Spark ne se connecte pas directement aux bases relationnelles.  
        Il utilise JDBC pour :
        - Lire les tables
        - Les transformer en DataFrame distribués
        - Les intégrer dans le pipeline Silver

        ### Justification métier

        PostgreSQL contient :
        - Régions administratives
        - Infrastructures rurales
        - Subventions agricoles

        Ces données structurées sont nécessaires pour enrichir les données agricoles brutes.
        """
    )
    return


@app.cell
def __():
    postgres_url = "jdbc:postgresql://postgres:5432/agrodb"

    properties = {
        "user": "agro",
        "password": "agro",
        "driver": "org.postgresql.Driver"
    }
    return postgres_url, properties


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Ingestion des données climatiques via API

        Cette cellule interroge une API Flask déployée contenant des données climatiques historiques et prospectives.

        ### Pourquoi une API ?

        - Une source externe réelle
        - Une intégration multi-systèmes
        - Un cas d’architecture distribuée

        ### Données récupérées

        - Température moyenne annuelle
        - Précipitations
        - Humidité
        - Projection climatique 2050
        - Élévation du niveau de la mer

        ### Justification métier

        Le climat est un déterminant majeur de la production agricole.

        Ces données permettront :
        - Analyse corrélative rendement/climat
        - Calcul d’indicateur de vulnérabilité
        - Simulation prospective
        """
    )
    return


@app.cell
def __():
    import requests
    import json
    from pyspark.sql import Row

    url = "https://ousmanefaye.pythonanywhere.com/climate/all"

    response = requests.get(url)
    climate_json = response.json()

    climate_json.keys()
    return Row, climate_json, json, requests, response, url


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Création des buckets Data Lake

        Cette cellule crée les buckets :

        - bronze
        - silver
        - gold

        ### Architecture Medallion

        Bronze → données brutes  
        Silver → données nettoyées et enrichies  
        Gold → données agrégées et décisionnelles  

        ### Pourquoi créer dynamiquement ?

        Cela garantit :
        - Reproductibilité
        - Déploiement automatisé
        - Infrastructure as Code

        ### Justification technique

        MinIO agit comme un stockage objet distribué, simulant un environnement cloud (S3).
        """
    )
    return


@app.cell
def __():
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123"
    )

    bucket_bronze = "bronze"

    existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]

    if bucket_bronze not in existing_buckets:
        s3.create_bucket(Bucket=bucket_bronze)
        print("Bucket bronze créé.")
    else:
        print("Bucket bronze existe déjà.")

    bucket_silver = "silver"

    existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]

    if bucket_silver not in existing_buckets:
        s3.create_bucket(Bucket=bucket_silver)
        print("Bucket silver créé.")
    else:
        print("Bucket silver existe déjà.")

    bucket_gold = "gold"

    existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]

    if bucket_gold not in existing_buckets:
        s3.create_bucket(Bucket=bucket_gold)
        print("Bucket gold créé.")
    else:
        print("Bucket gold existe déjà.")
    return (
        boto3,
        bucket_bronze,
        bucket_gold,
        bucket_silver,
        existing_buckets,
        s3,
    )


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Détection des fichiers agricoles CSV

        Nous listons dynamiquement les fichiers CSV contenant les données agricoles.

        ### Pourquoi dynamique ?

        Cela permet :
        - Scalabilité
        - Ajout futur de nouvelles cultures sans modifier le code
        - Flexibilité pipeline

        Chaque fichier correspond à une culture.
        """
    )
    return


@app.cell
def _(os):
    base_path = "/app/data/agriculture"

    files = [f for f in os.listdir(base_path) if f.endswith(".csv")]

    files
    return base_path, files


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Correction des anomalies d’encodage des régions

        Certaines régions contiennent des caractères mal encodés.

        Exemple :
        - "This" → "Thiès"
        - "SŽdhiou" → "Sédhiou"

        ### Pourquoi corriger ici ?

        Les jointures ultérieures (Silver) dépendent de la cohérence des clés de jointure.

        Sans harmonisation :
        - Les jointures échoueraient
        - Certaines régions seraient exclues

        ### Justification métier

        Une mauvaise qualité des noms régionaux entraîne :
        - Perte d’information
        - Mauvaise agrégation territoriale
        - Erreurs analytiques
        """
    )
    return


@app.cell
def __():
    region_corrections = {
        "SŽdhiou": "Sédhiou",
        "This": "Thiès",
        "KŽdougou": "Kédougou"
    }
    region_corrections
    return (region_corrections,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Transformation des données agricoles en format long

        Les données originales sont en format large (wide) :

        superf_ha_2003, superf_ha_2004, ...

        Nous les transformons en format long :

        region | culture | annee | superficie | rendement | production

        ### Pourquoi ?

        Le format long est :
        - Plus adapté aux jointures
        - Compatible avec les analyses temporelles
        - Optimisé pour Spark et Parquet

        ### Justification métier

        Un modèle analytique nécessite une dimension temporelle claire.

        Cela permet :
        - Analyse annuelle
        - Agrégation multi-années
        - Calcul d’indicateurs temporels
        """
    )
    return


@app.cell
def _(base_path, col, files, lit, os, region_corrections, spark):
    final_df = None

    years = list(range(2003, 2013))

    for file in files:

        culture = file.split("-")[0]
        file_path = os.path.join(base_path, file)

        df = spark.read.csv(
            file_path,
            header=True,
            inferSchema=True,
            sep=";",
            encoding="UTF-8"
        )

        # Correction encoding régions
        for wrong, correct in region_corrections.items():
            df = df.replace(wrong, correct, subset=["region"])

        culture_long = None

        for year in years:

            temp = df.select(
                col("region"),
                lit(culture).alias("culture"),
                lit(year).alias("annee"),
                col(f"superf_ha_{year}").alias("superficie_ha"),
                col(f"rdt_kg_ha_{year}").alias("rendement_kg_ha"),
                col(f"prod_tonn_{year}").alias("production_tonnes")
            )

            if culture_long is None:
                culture_long = temp
            else:
                culture_long = culture_long.union(temp)

        if final_df is None:
            final_df = culture_long
        else:
            final_df = final_df.union(culture_long)

    final_df
    return (
        correct,
        culture,
        culture_long,
        df,
        file,
        file_path,
        final_df,
        temp,
        wrong,
        year,
        years,
    )


@app.cell
def __(final_df):
    final_df.count()
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Contrôle qualité : vérification de cohérence

        Nous recalculons :

        production = superficie × rendement / 1000

        Puis nous comparons avec la valeur officielle.

        ### Objectif

        Détecter :
        - Erreurs de saisie
        - Incohérences statistiques
        - Valeurs aberrantes

        ### Pourquoi c’est important ?

        La production agricole est un indicateur central.

        Une incohérence impacte :
        - Calcul sécurité alimentaire
        - Indice de performance
        - Indicateurs Gold
        """
    )
    return


@app.cell
def __(abs, col, final_df):
    check_df = final_df.withColumn(
        "production_calculee",
        (col("superficie_ha") * col("rendement_kg_ha")) / 1000
    ).withColumn(
        "ecart",
        abs(col("production_tonnes") - col("production_calculee"))
    )

    check_df.orderBy(col("ecart").desc()).show(5)
    return (check_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Création du dataset Bronze Agriculture

        Nous sélectionnons uniquement :

        - region
        - culture
        - annee
        - superficie
        - rendement
        - production

        ### Pourquoi simplifier ?

        La couche Bronze doit :
        - Rester brute
        - Être minimale
        - Être réutilisable

        ### Stockage en Parquet

        Parquet est :
        - Colonnaire
        - Compressé
        - Optimisé analytique
        """
    )
    return


@app.cell
def __(final_df):
    bronze_df = final_df.select(
        "region",
        "culture",
        "annee",
        "superficie_ha",
        "rendement_kg_ha",
        "production_tonnes"
    )

    bronze_df.printSchema()
    bronze_df.show(10)
    return (bronze_df,)


@app.cell
def __(bronze_df):
    bronze_df.write \
        .mode("overwrite") \
        .parquet("s3a://bronze/agriculture/")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Ingestion des données économiques PostgreSQL

        Nous lisons les tables :

        - regions
        - infrastructures
        - subventions_agricoles

        Puis nous les stockons dans Bronze.

        ### Pourquoi les placer en Bronze ?

        Même si elles sont propres, elles constituent :
        - Une source brute externe
        - Un point d’entrée dans le Data Lake

        Cela garantit :
        - Centralisation
        - Traçabilité
        - Cohérence architecture
        """
    )
    return


@app.cell
def __(postgres_url, properties, spark):
    regions_df = spark.read.jdbc(
        url=postgres_url,
        table="regions",
        properties=properties
    )

    infra_df = spark.read.jdbc(
        url=postgres_url,
        table="infrastructures",
        properties=properties
    )

    subventions_df = spark.read.jdbc(
        url=postgres_url,
        table="subventions_agricoles",
        properties=properties
    )
    return infra_df, regions_df, subventions_df


@app.cell
def __(infra_df, regions_df, subventions_df):
    regions_df.show(15)
    infra_df.show(15)
    subventions_df.show(15)
    return


@app.cell
def __(infra_df, regions_df, subventions_df):
    regions_df.write.mode("overwrite").parquet("s3a://bronze/data_eco/regions/")
    infra_df.write.mode("overwrite").parquet("s3a://bronze/data_eco/infrastructures/")
    subventions_df.write.mode("overwrite").parquet("s3a://bronze/data_eco/subventions/")
    return


@app.cell
def __(infra_df, regions_df, subventions_df):
    regions_df.printSchema()
    infra_df.printSchema()
    subventions_df.printSchema()
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Relecture Bronze Agriculture

        Nous rechargeons les données Bronze pour validation.

        ### Pourquoi relire ?

        Cela simule :
        - Un pipeline modulaire
        - Une séparation des étapes
        - Une architecture reproductible

        Chaque couche doit pouvoir être reconstruite indépendamment.
        """
    )
    return


@app.cell
def __(spark):
    bronze_agri_df = spark.read.parquet("s3a://bronze/agriculture/")
    bronze_agri_df.printSchema()
    return (bronze_agri_df,)


@app.cell
def __(bronze_df):
    silver_agri_df = bronze_df
    return (silver_agri_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Construction de la couche Silver : jointures multi-sources

        Cette section réalise les jointures principales entre :

        - Données agricoles (Bronze)
        - Données territoriales (régions)
        - Données d’infrastructures
        - Données de subventions

        ### 1 Jointure Agriculture ↔ Régions

        Clé de jointure :
        - agri.region = reg.nom_region

        Type :
        - LEFT JOIN

        Pourquoi LEFT JOIN ?
        - Conserver toutes les données agricoles
        - Même si une région manque dans PostgreSQL
        - Éviter la perte d’information métier

        Enrichissements ajoutés :
        - region_id (clé technique)
        - code_region
        - zone_agro_ecologique
        - pluviometrie_moyenne
        - population_rurale

         Cette étape introduit la dimension territoriale et démographique.

        ---

        ### 2️ Jointure avec Infrastructures

        Clé composite :
        - region_id
        - annee

        Pourquoi clé composite ?
        Les infrastructures évoluent dans le temps.
        On ne peut pas joindre uniquement sur la région.

        Enrichissements :
        - nb_forages
        - nb_centres_stockage
        - km_routes_rurales

        Justification métier :
        Les infrastructures influencent directement :
        - Stockage post-récolte
        - Irrigation
        - Accessibilité des marchés

        ---

        ### 3️ Jointure avec Subventions Agricoles

        Clé composite :
        - region_id
        - annee

        Enrichissements :
        - montant_total_fcfa
        - nb_beneficiaires
        - type_subvention

        Justification métier :
        Permet d’évaluer :
        - Impact des politiques publiques
        - Efficacité des subventions
        - Retour sur investissement agricole

        ---

        Résultat :
        Un dataset Silver unifié multi-dimensionnel prêt pour analyse avancée.
        """
    )
    return


@app.cell
def __(infra_df, regions_df, silver_agri_df, subventions_df):
    from pyspark.sql import functions as F

    # Première jointure
    silver_df = silver_agri_df.alias("agri").join(
        regions_df.alias("reg"),
        F.col("agri.region") == F.col("reg.nom_region"),
        "left"
    ).select(
        "agri.*",
        F.col("reg.id").alias("region_id"),  # Renommer id en region_id
        "reg.nom_region",
        "reg.code_region",
        "reg.zone_agro_ecologique",
        "reg.pluviometrie_moyenne",
        "reg.population_rurale"
    )

    # Deuxième jointure avec infra_df
    silver_df = silver_df.alias("combined").join(
        infra_df.alias("infra"),
        (F.col("combined.region_id") == F.col("infra.region_id")) &
        (F.col("combined.annee") == F.col("infra.annee")),
        "left"
    ).select(
        "combined.*",
        F.col("infra.id").alias("infra_id"),  
        "infra.nb_forages",
        "infra.nb_centres_stockage",
        "infra.km_routes_rurales"
    )


    silver_df = silver_df.alias("combined2").join(
        subventions_df.alias("sub"),
        (F.col("combined2.region_id") == F.col("sub.region_id")) &
        (F.col("combined2.annee") == F.col("sub.annee")),
        "left"
    ).select(
        "combined2.*",
        F.col("sub.id").alias("subvention_id"),  
        "sub.montant_total_fcfa",
        "sub.nb_beneficiaires",
        "sub.type_subvention"
    )

    silver_df.show(5)
    return F, silver_df


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Création d’indicateurs intermédiaires métier

        Deux indicateurs clés sont ajoutés.

        ---

        ### 1️ Valeur subvention par bénéficiaire

        Formule :

        montant_total_fcfa / nb_beneficiaires

        Objectif :
        Mesurer l’intensité réelle du soutien financier.

        Pourquoi ?
        Le montant total seul n’est pas interprétable.
        Diviser par le nombre de bénéficiaires permet :
        - Comparaison inter-régionale
        - Évaluation d’équité

        ---

        ### 2️ Production par habitant rural

        Formule :

        production_tonnes / population_rurale

        Objectif :
        Mesurer la capacité productive par individu rural.

        Pourquoi ?
        Permet d’évaluer :
        - Sécurité alimentaire
        - Autosuffisance régionale
        - Pression démographique

        ---

        Ces indicateurs constituent une base analytique pour la couche Gold.
        """
    )
    return


@app.cell
def __(col, silver_df):
    silver_dfs = silver_df.withColumn(
        "valeur_subvention_par_beneficiaire",
        col("montant_total_fcfa") / col("nb_beneficiaires")
    )

    silver_dfs = silver_dfs.withColumn(
        "production_par_habitant_rural",
        col("production_tonnes") / col("population_rurale")
    )
    return (silver_dfs,)


@app.cell
def __(silver_dfs):
    silver_dfs.show(5)
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Stockage de la couche Silver

        Les données enrichies sont sauvegardées dans le bucket :

        silver/agriculture_enrichi/

        Format : Parquet

        Pourquoi Parquet ?
        - Stockage colonnaire
        - Compression efficace
        - Optimisé pour les requêtes analytiques

        Pourquoi séparer Bronze et Silver ?
        - Respect de l’architecture Medallion
        - Isolation des responsabilités
        - Rejouabilité des transformations
        """
    )
    return


@app.cell
def __(silver_dfs):
    silver_dfs.write.mode("overwrite").parquet("s3a://silver/agriculture_enrichi/")
    return


@app.cell
def __(silver_dfs):
    silver_dfs.printSchema()
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Transformation des données climatiques en DataFrame Spark

        Les données climatiques issues de l’API sont initialement en JSON.

        Étapes réalisées :
        1. Conversion en liste de Row
        2. Création d’un DataFrame Spark
        3. Harmonisation des noms de régions

        ---

        ### Pourquoi harmoniser les noms ?

        Les clés de jointure doivent être strictement identiques.

        Exemple :
        - Kedougou → Kédougou
        - Thies → Thiès

        Sans harmonisation :
        - Jointure échouée
        - Données climatiques non associées
        - Analyse biaisée

        ---

        Cette étape prépare l’intégration climat ↔ agriculture.
        """
    )
    return


@app.cell
def __(Row, climate_json, col, spark, when):
    climate_rows = []

    for region, values in climate_json.items():
        row = {"region": region}
        row.update(values)
        climate_rows.append(Row(**row))

    climate_df = spark.createDataFrame(climate_rows)

    climate_df = climate_df.withColumn(
        "region",
        when(col("region") == "Kedougou", "Kédougou")
        .when(col("region") == "Sedhiou", "Sédhiou")
        .when(col("region") == "Thies", "Thiès")
        .otherwise(col("region"))
    )

    climate_df.printSchema()
    climate_df.show(5)
    return climate_df, climate_rows, region, row, values


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Stockage des données climatiques en Bronze

        Même si elles proviennent d’une API,
        elles sont stockées en Bronze avant transformation.

        Pourquoi ?
        - Respect de l’architecture Medallion
        - Traçabilité
        - Possibilité de rejouer les transformations
        - Centralisation des sources

        Le Bronze reste la source brute officielle.
        """
    )
    return


@app.cell
def __(climate_df):
    climate_df.write.mode("overwrite").parquet("s3a://bronze/climate_data/")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Enrichissement climatique final (Silver étendu)

        Jointure sur :

        region

        Type :
        LEFT JOIN

        Pourquoi LEFT JOIN ?
        Toutes les données agricoles doivent être conservées,
        même si certaines régions n’ont pas de données climatiques complètes.

        Résultat :
        Dataset complet agriculture + économie + climat.
        """
    )
    return


@app.cell
def __(climate_df, silver_dfs):
    silver_full_df = silver_dfs.join(
        climate_df,
        on="region",
        how="left"
    )
    return (silver_full_df,)


@app.cell
def __(silver_full_df):
    silver_full_df.show(5)
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Construction des indicateurs stratégiques Gold

        Cette section crée des indicateurs décisionnels avancés.

        ---

        ### 1️ Climate Risk Index

        Formule :

        (1000 - précipitations) * 0.3
        + température * 2
        + |variation précipitation 2050| * 5

        Objectif :
        Créer un score synthétique de vulnérabilité climatique.

        Logique métier :
        - Moins de pluie → risque plus élevé
        - Température élevée → stress thermique
        - Forte variation future → incertitude accrue

        ---

        ### 2️ Climate Adjusted Yield

        Formule :

        rendement / (température / 25)

        Objectif :
        Normaliser le rendement selon les conditions climatiques.

        Interprétation :
        Permet de comparer les performances indépendamment du climat.

        ---

        ### 3️ Economic Vulnerability Score

        Formule :

        (production_par_habitant_rural / précipitations) * 100

        Objectif :
        Identifier les régions dépendantes d’un climat fragile.

        Interprétation :
        Faible production + faible pluie → vulnérabilité élevée
        """
    )
    return


@app.cell
def __(abs, col, silver_full_df):
    gold_climate_df = silver_full_df.withColumn(
        "climate_risk_index",
        (
            (1000 - col("annual_avg_precip")) * 0.3 +
            col("annual_avg_temp") * 2 +
            abs(col("projected_precip_change_2050")) * 5
        )
    )
    gold_climate_df = gold_climate_df.withColumn(
        "climate_adjusted_yield",
        col("rendement_kg_ha").cast("double") /
        (col("annual_avg_temp") / 25)
    )

    gold_climate_df = gold_climate_df.withColumn(
        "economic_vulnerability_score",
        (
            col("production_par_habitant_rural") /
            col("annual_avg_precip")
        ) * 100
    )
    gold_climate_df.show(5)
    return (gold_climate_df,)


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Agrégation régionale – Performance stratégique

        Les données sont agrégées par région pour produire une vue synthétique.

        Indicateurs calculés :

        - Production moyenne
        - Risque climatique moyen
        - Efficacité moyenne des subventions

        Pourquoi agréger ?

        La couche Gold vise :
        - Décideurs publics
        - Analyse stratégique
        - Comparaison inter-régionale

        Elle n’est plus orientée ligne opérationnelle,
        mais vue macro-économique.
        """
    )
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Stockage Gold optimisé

        Les données sont :

        - Stockées en Parquet
        - Partitionnées par région

        Pourquoi partitionner ?

        Avantages :
        - Optimisation des requêtes
        - Lecture ciblée
        - Performance analytique accrue

        Exemple :
        Un dashboard qui interroge uniquement "Dakar"
        ne lira que la partition correspondante.

        Cela simule une architecture Data Warehouse moderne.
        """
    )
    return


@app.cell
def __(avg, gold_climate_df):
    gold_regional = gold_climate_df.groupBy("region").agg(
        avg("production_tonnes").alias("avg_production"),
        avg("climate_risk_index").alias("avg_climate_risk"),
        avg("valeur_subvention_par_beneficiaire").alias("avg_subvention_efficiency")
    )
    return (gold_regional,)


@app.cell
def __(gold_regional):
    gold_regional.write.mode("overwrite") \
        .partitionBy("region") \
        .parquet("s3a://gold/regional_performance/")
    return


@app.cell(hide_code=True)
def __(mo):
    mo.md(
        r"""
        ## Conclusion Générale du Projet

        Ce projet a consisté à concevoir et implémenter une **Data Platform complète** appliquée à l’analyse croisée des données agricoles, socio-économiques et climatiques du Sénégal.

        L’architecture mise en place repose sur le modèle **Medallion (Bronze → Silver → Gold)**, garantissant :

        - Séparation claire des responsabilités
        - Traçabilité des transformations
        - Reproductibilité des pipelines
        - Scalabilité analytique

        ---

        ## Synthèse Technique

        ### Outils et Technologies Mobilisés

        - **PySpark** : Traitement distribué et transformation des données
        - **PostgreSQL** : Source OLTP structurée (données territoriales et économiques)
        - **API Flask** : Source externe climatique simulant un système tiers
        - **MinIO (S3-compatible)** : Stockage objet Data Lake
        - **Parquet** : Format colonnaire optimisé pour l’analytique
        - **Docker Compose** : Orchestration des services
        - **Marimo** : Notebook interactif pour le développement reproductible

        ---

        ## Architecture Implémentée

        ### Bronze
        - Centralisation des données brutes multi-sources
        - Conservation des données sans logique métier
        - Garantie de traçabilité

        ### Silver
        - Nettoyage et harmonisation des clés de jointure
        - Intégration multi-dimensionnelle :
          - Agriculture
          - Régions
          - Infrastructures
          - Subventions
          - Climat
        - Création d’indicateurs intermédiaires

        ### Gold
        - Construction d’indicateurs décisionnels avancés
        - Agrégation régionale
        - Optimisation par partitionnement
        - Production d’une vue stratégique exploitable

        ---

        ## Apport du Point de Vue Data Engineering

        Ce projet démontre :

        - La capacité à intégrer des sources hétérogènes (CSV, API, base relationnelle)
        - La mise en place d’un pipeline reproductible
        - L’application concrète d’une architecture cloud-native
        - L’utilisation de standards industriels (S3, Parquet, JDBC)
        - La gestion de la qualité des données (contrôle de cohérence, harmonisation)

        Il s’inscrit pleinement dans une logique de **Data Engineering moderne orientée production**.

        ---

        ## Impact Métier

        D’un point de vue métier, cette plateforme permet :

        - L’évaluation de l’efficacité des subventions agricoles
        - L’analyse de la vulnérabilité climatique régionale
        - La mesure de la productivité ajustée aux conditions climatiques
        - L’identification des zones à risque alimentaire
        - L’aide à la priorisation des investissements publics

        Elle fournit ainsi un cadre analytique robuste pour :

        - La planification agricole
        - La résilience climatique
        - L’optimisation des politiques publiques

        ---

        ## Vision Stratégique

        Ce projet constitue une base solide pour :

        - L’intégration future de données satellitaires (NDVI, NDWI)
        - L’ajout de modèles prédictifs (Machine Learning)
        - Le déploiement d’un Data Warehouse analytique
        - La mise en place d’un dashboard décisionnel

        Il illustre la convergence entre **Data Engineering, analyse territoriale et stratégie publique**, démontrant comment une architecture data bien conçue peut transformer des données brutes en leviers décisionnels concrets.

        ---

        ### En résumé

        Ce projet ne se limite pas à un exercice technique :  
        Il matérialise une plateforme analytique complète capable de soutenir des décisions stratégiques dans un contexte réel d’agriculture et de changement climatique.
        """
    )
    return


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
