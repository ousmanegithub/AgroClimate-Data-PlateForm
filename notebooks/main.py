import marimo

__generated_with = "0.8.22"
app = marimo.App()


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


@app.cell
def __():
    postgres_url = "jdbc:postgresql://postgres:5432/agrodb"

    properties = {
        "user": "agro",
        "password": "agro",
        "driver": "org.postgresql.Driver"
    }
    return postgres_url, properties


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


@app.cell
def _(os):
    base_path = "/app/data/agriculture"

    files = [f for f in os.listdir(base_path) if f.endswith(".csv")]

    files
    return base_path, files


@app.cell
def __():
    region_corrections = {
        "SŽdhiou": "Sédhiou",
        "This": "Thiès",
        "KŽdougou": "Kédougou"
    }
    region_corrections
    return (region_corrections,)


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


@app.cell
def __(spark):
    bronze_agri_df = spark.read.parquet("s3a://bronze/agriculture/")
    bronze_agri_df.printSchema()
    return (bronze_agri_df,)


@app.cell
def __(bronze_df):
    silver_agri_df = bronze_df
    return (silver_agri_df,)


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


@app.cell
def __(silver_dfs):
    silver_dfs.write.mode("overwrite").parquet("s3a://silver/agriculture_enrichi/")
    return


@app.cell
def __(silver_dfs):
    silver_dfs.printSchema()
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


@app.cell
def __(climate_df):
    climate_df.write.mode("overwrite").parquet("s3a://bronze/climate_data/")
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


@app.cell
def __():
    return


if __name__ == "__main__":
    app.run()
