from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
                    .appName("CPT Codes Ingestion") \
                    .getOrCreate()

# configurar variables
BUCKET_NAME = "healthcare-bucket-22032025"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "avd-databricks-demo.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# leer los códigos CPT
cptcodes_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

# reemplazar espacios por guion bajo
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# escribir en bigquery
(cptcodes_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())