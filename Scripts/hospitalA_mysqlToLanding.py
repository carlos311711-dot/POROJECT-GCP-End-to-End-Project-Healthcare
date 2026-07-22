from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json

# Inicializar los clientes de GCS y BigQuery
storage_client = storage.Client()
bq_client = bigquery.Client()

# Inicializar la sesión de Spark
spark = SparkSession.builder.appName("HospitalAMySQLToLanding").getOrCreate()

# Configuración de Google Cloud Storage (GCS)
GCS_BUCKET = "healthcare-bucket-22032025"
HOSPITAL_NAME = "hospital-a"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

# Configuración de BigQuery
BQ_PROJECT = "avd-databricks-demo"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

# Configuración de MySQL
MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.132.104.87:3306/hospital_a_db?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "mypass"
}

##------------------------------------------------------------------------------------------------------------------##
# Mecanismo de logging
log_entries = []  # Guarda los logs antes de escribirlos en GCS

def log_event(event_type, message, table=None):
    """Registra un evento y lo guarda en la lista de logs"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")  # Imprimir para visibilidad

def save_logs_to_gcs():
    """Guarda los logs en un archivo JSON y lo sube a GCS"""
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    # Obtener el bucket de GCS
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(log_filepath)

    # Subir los datos JSON como archivo
    blob.upload_from_string(json_data, content_type="application/json")

    print(f"✅ Logs guardados exitosamente en GCS en gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    """Guarda los logs en BigQuery"""
    if log_entries:
        log_df = spark.createDataFrame(log_entries)
        log_df.write.format("bigquery") \
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
        print("✅ Logs almacenados en BigQuery para análisis futuro")

##------------------------------------------------------------------------------------------------------------------##

# Función para mover los archivos existentes al archivo histórico (archive)
def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No hay archivos existentes para la tabla {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extraer la fecha del nombre del archivo
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        # Mover al archivo histórico
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copiar el archivo al histórico y eliminar el original
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()

        log_event("INFO", f"Se movió {file} a {archive_path}", table=table)

##------------------------------------------------------------------------------------------------------------------##

# Función para obtener el último watermark desde la tabla de auditoría en BigQuery
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' and data_source = "hospital_a_db"
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"

##------------------------------------------------------------------------------------------------------------------##

# Función para extraer datos de MySQL y guardarlos en GCS
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Último watermark para {table}: {last_watermark}", table=table)

        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())

        log_event("SUCCESS", f"✅ Datos extraídos exitosamente de {table}", table=table)

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")

        log_event("SUCCESS", f"✅ Archivo JSON escrito exitosamente en gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)

        # Insertar el registro de auditoría
        audit_df = spark.createDataFrame([
            ("hospital_a_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")],
            ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

        (audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save())

        log_event("SUCCESS", f"✅ Log de auditoría actualizado para {table}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error al procesar {table}: {str(e)}", table=table)
##------------------------------------------------------------------------------------------------------------------##

# Función para leer el archivo de configuración desde GCS
def read_config_file():
    df = spark.read.csv(CONFIG_FILE_PATH, header=True)
    log_event("INFO", "✅ Archivo de configuración leído exitosamente")
    return df

# leer el archivo de configuración
config_df = read_config_file()

for row in config_df.collect():
    if row["is_active"] == '1' and row["datasource"] == "hospital_a_db":
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()
