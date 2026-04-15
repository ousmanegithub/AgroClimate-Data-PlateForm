from scripts.utiles import get_spark_session
from pyspark.sql.functions import col, lit, abs
import os

def run_ingestion():
    spark = get_spark_session("AgroBronze_Ingestion")
    base_path = "/app/data/agriculture" # Vérifie que ce chemin est accessible au worker Airflow
    files = [f for f in os.listdir(base_path) if f.endswith(".csv")]
    
    region_corrections = {"SÅ½dhiou": "Sédhiou", "ThiÂ s": "Thiès", "KÅ½dougou": "Kédougou"}
    years = list(range(2003, 2013))
    final_df = None

    for file in files:
        culture = file.split("-")[0]
        df = spark.read.csv(os.path.join(base_path, file), header=True, inferSchema=True, sep=";", encoding="UTF-8")

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
            culture_long = temp if culture_long is None else culture_long.union(temp)
        
        final_df = culture_long if final_df is None else final_df.union(culture_long)

    # Sauvegarde vers Bronze en format Parquet sur MinIO
    final_df.write.mode("overwrite").parquet("s3a://bronze/agriculture_brute.parquet")
    print("Données agriculture chargées dans le bucket Bronze.")

if __name__ == "__main__":
    run_ingestion()
