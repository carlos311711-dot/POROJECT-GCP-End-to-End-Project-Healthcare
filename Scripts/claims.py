from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

# Crear sesión de Spark
spark = SparkSession.builder \
                    .appName("Healthcare Claims Ingestion") \
                    .getOrCreate()

# configurar variables
BUCKET_NAME = "healthcare-bucket-22032025"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "avd-databricks-demo.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# leer desde el origen de reclamaciones (claims)
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# agregar el hospital de origen para referencia futura
claims_df = (claims_df
                .withColumn("datasource",
                              when(input_file_name().contains("hospital2"), "hosb")
                             .when(input_file_name().contains("hospital1"), "hosa").otherwise("None")))

# eliminar duplicados si los hay
claims_df = claims_df.dropDuplicates()

# escribir en bigquery
(claims_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", TEMP_GCS_BUCKET)
            .mode("overwrite")
            .save())