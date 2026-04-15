import requests
from scripts.utiles import get_spark_session
from pyspark.sql import Row

def ingest_climate_api():
    # Initialisation de la session Spark orientée S3
    spark = get_spark_session("ClimateIngestion")
    url = "https://ousmanefaye.pythonanywhere.com/climate/all"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        climate_data = response.json()

        # Transformation du JSON en liste de Rows Spark
        # On suppose que le JSON est un dictionnaire de régions
        rows = []
        for region, data in climate_data.items():
            # On crée un dictionnaire plat pour Spark
            row_dict = {"region": region}
            row_dict.update(data)
            rows.append(Row(**row_dict))

        df = spark.createDataFrame(rows)

        # Sauvegarde en Bronze (Overwrite pour éviter les doublons au retry)
        df.write.mode("overwrite").parquet("s3a://bronze/climate_brute.parquet")
        print(f"Succès : {df.count()} régions climatiques importées.")

    except Exception as e:
        print(f"Erreur lors de l'ingestion API : {e}")
        raise e

if __name__ == "__main__":
    ingest_climate_api()
