# MANUAL DE INGESTA DE DATOS: DATAPROC, PYSPARK Y GCS LANDING
## Extracción de Datos EMR Relacionales a la Landing Zone de Google Cloud Storage

> [!NOTE]
> ### 📍 Ubicación del Código y Scripts Python
> Los scripts de PySpark que implementan esta ingesta están ubicados en:
> * **Extracción Hospital A (JDBC):** [Scripts/hospitalA_mysqlToLanding.py](../Scripts/hospitalA_mysqlToLanding.py)
> * **Extracción Hospital B (JDBC):** [Scripts/hospitalB_mysqlToLanding.py](../Scripts/hospitalB_mysqlToLanding.py)
> * **Ingesta de Aseguradoras (Claims):** [Scripts/claims.py](../Scripts/claims.py)
> * **Ingesta de Códigos de Diagnóstico (CPT):** [Scripts/cpt_codes.py](../Scripts/cpt_codes.py)
>
> ### ⚙️ Cómo Ejecutar
> Estos scripts se ejecutan como Jobs de PySpark dentro de tu clúster de Dataproc mediante el comando de `gcloud`:
> ```bash
> gcloud dataproc jobs submit pyspark Scripts/hospitalA_mysqlToLanding.py \
>     --cluster=my-demo-cluster2 \
>     --region=us-central1
> ```

Este manual explica detalladamente la fase de **Ingesta de Datos (Fase 1)** utilizando **Google Cloud Dataproc** y scripts de **PySpark**. A través de este proceso, los datos alojados en las bases de datos transaccionales (Cloud SQL MySQL) del Hospital A y B son extraídos de manera óptima (soportando cargas completas e incrementales) y almacenados en formato estructurado JSON Lines en la Landing Zone de **Google Cloud Storage (GCS)**.

---

## 📖 Tabla de Contenidos
1. [¿Por qué utilizar Cloud Dataproc y PySpark?](#1-por-qué-utilizar-cloud-dataproc-y-pyspark)
2. [Paso 1: Creación del Clúster de Dataproc](#paso-1-creación-del-clúster-de-dataproc)
3. [Paso 2: Inicialización de la Sesión y Configuración de Clientes](#paso-2-inicialización-de-la-sesión-y-configuración-de-clientes)
4. [Paso 3: Mecanismo de Archivado Histórico (Archive) en GCS](#paso-3-mecanismo-de-archivado-histórico-archive-en-gcs)
5. [Paso 4: Lógica de Watermarking (Carga Incremental)](#paso-4-lógica-de-watermarking-carga-incremental)
6. [Paso 5: Extracción JDBC y Escritura en GCS Landing](#paso-5-extracción-jdbc-y-escritura-en-gcs-landing)
7. [Paso 6: Auditoría y Logging del Pipeline](#paso-6-auditoría-y-logging-del-pipeline)

---

## 1. ¿Por qué utilizar Cloud Dataproc y PySpark?

En proyectos de Big Data, la ingesta desde bases de datos operacionales EMR hacia el Data Lake puede congestionar los servidores relacionales si se realiza de forma directa o poco eficiente. 

* **Cloud Dataproc:** Es el servicio totalmente gestionado de GCP para ejecutar clústeres de Apache Spark y Hadoop de forma elástica y rentable.
* **PySpark:** Permite utilizar la potencia de procesamiento distribuido de Spark a través del lenguaje Python, ideal para leer de MySQL mediante JDBC en paralelo, procesar la información y transformarla eficientemente.

---

## Paso 1: Creación del Clúster de Dataproc

Para ejecutar nuestros jobs de PySpark, debemos contar con un clúster activo en Dataproc.

### A. Creación desde la Consola Web de GCP
1. Ve a **Dataproc** > **Clústeres** en la consola de GCP y haz clic en **Crear Clúster** (selecciona *Clúster en Compute Engine*).
2. Configura los datos básicos:
   - **Nombre del clúster:** `my-demo-cluster2`
   - **Región:** `us-central1`
   - **Zona:** `us-central1-a`
   - **Tipo de clúster:** Single Node (1 Master, 0 Workers) es suficiente para entornos de prueba/desarrollo; Standard (1 Master, N Workers) para producción.
3. En la sección **Configurar componentes de software**, habilita el componente opcional de **Jupyter Web Portal (JupyterLab)** para desarrollo interactivo si es necesario.

### B. Creación Rápida mediante Cloud Shell (Línea de Comandos)
Para evitar la configuración manual paso a paso, se puede desplegar el clúster con un solo comando de `gcloud` desde Cloud Shell:

--ip publica
```bash

gcloud dataproc clusters create my-demo-cluster2 \
    --enable-component-gateway \
    --region us-east1 \
    --zone us-east1-c \
    --master-machine-type e2-standard-2 \
    --master-boot-disk-size 50 \
    --master-boot-disk-type pd-standard \
    --num-workers 2 \
    --worker-machine-type e2-standard-2 \
    --worker-boot-disk-size 50 \
    --worker-boot-disk-type pd-standard \
    --image-version 2.1-debian11 \
    --optional-components JUPYTER \
    --project project-d92eee7b-8c90-4381-b63
```
--ip privada

```bash
gcloud dataproc clusters create my-demo-cluster2 \
    --enable-component-gateway \
    --no-address \
    --region us-east1 \
    --zone us-east1-c \
    --master-machine-type e2-standard-2 \
    --master-boot-disk-size 50 \
    --master-boot-disk-type pd-standard \
    --num-workers 2 \
    --worker-machine-type e2-standard-2 \
    --worker-boot-disk-size 50 \
    --worker-boot-disk-type pd-standard \
    --image-version 2.1-debian11 \
    --optional-components JUPYTER \
    --project project-d92eee7b-8c90-4381-b63
```
*Una vez creado (tardará entre 3 y 5 minutos), el clúster estará listo para recibir jobs de Spark o para trabajar en JupyterLab desde la pestaña "Interfaces Web" de Dataproc.*

---

## Paso 2: Inicialización de la Sesión y Configuración de Clientes

Cada script de PySpark requiere configurar el entorno de ejecución, los endpoints de las fuentes y destinos, y los clientes nativos de GCP:

```python
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
import json

# 1. Inicializar Clientes de API nativos de GCP
storage_client = storage.Client()
bq_client = bigquery.Client()

# 2. Inicializar Sesión Spark
spark = SparkSession.builder \
    .appName("HospitalAMySQLToLanding") \
    .getOrCreate()

# 3. Configuración de GCS Bucket y Rutas
GCS_BUCKET = "healthcare-bucket-22032025"
HOSPITAL_NAME = "hospital-a"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"

# 4. Configuración de BigQuery para Metadata y Auditoría
BQ_PROJECT = "avd-databricks-demo"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
```

---

## Paso 3: Mecanismo de Archivado Histórico (Archive) en GCS

Antes de realizar la ingesta de los nuevos datos del día, el pipeline implementa un mecanismo de archivado para evitar sobrescribir datos del día anterior o perder historial de cargas en la landing zone:

```
[ landing/hospital-a/patients/patients_24032025.json ] (Archivo viejo)
                       │
                       ▼ Mover blob mediante API de GCS
[ landing/hospital-a/archive/patients/2025/03/24/patients_24032025.json ]
```

### Código de la Función de Archivado:
```python
def move_existing_files_to_archive(table):
    # Listar todos los archivos JSON actuales en la carpeta de la tabla en Landing
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    if not existing_files:
        log_event("INFO", f"No hay archivos existentes para archivar en la tabla {table}")
        return

    for file in existing_files:
        source_blob = storage_client.bucket(GCS_BUCKET).blob(file)

        # Extraer la fecha desde el nombre del archivo (ej. table_ddmmyyyy.json)
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]

        # Construir la ruta histórica de destino
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)

        # Copiar y eliminar el archivo original de la Landing
        storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
        source_blob.delete()

        log_event("INFO", f"Se archivó {file} en {archive_path}", table=table)
```

---

## Paso 4: Lógica de Watermarking (Carga Incremental)

Para las tablas de gran volumen (`patients`, `encounters`, `transactions`), no es eficiente extraer toda la base de datos todos los días. Usamos **cargas incrementales** basadas en una columna de control de fecha de modificación (`updated_at` / `watermark_column`):

1. **Obtener el último Watermark:** El script consulta a la tabla de auditoría central de BigQuery cuál fue el timestamp de la última carga exitosa para esta tabla específica de este hospital.
2. **Si no existen cargas previas:** Devuelve la fecha inicial por defecto `1900-01-01 00:00:00`, forzando a extraer el historial completo en la primera ejecución.

```python
def get_latest_watermark(table_name):
    query = f"""
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE tablename = '{table_name}' AND data_source = "hospital_a_db"
    """
    query_job = bq_client.query(query)
    result = query_job.result()
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"
```

---

## Paso 5: Extracción JDBC y Escritura en GCS Landing

La función principal extrae los datos correspondientes según el archivo de configuración y los guarda en GCS:

### 1. Construcción de Query Dinámica
Dependiendo del tipo de carga asignado en `load_config.csv`:
* **Carga Completa (Full):** `(SELECT * FROM {table}) AS t`
* **Carga Incremental:** Se obtiene el `last_watermark` y se filtra:
  ```sql
  (SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t
  ```

### 2. Extracción mediante Spark JDBC
Spark realiza la conexión en paralelo al servidor MySQL de Cloud SQL:
```python
df = (spark.read.format("jdbc")
        .option("url", MYSQL_CONFIG["url"])
        .option("user", MYSQL_CONFIG["user"])
        .option("password", MYSQL_CONFIG["password"])
        .option("driver", MYSQL_CONFIG["driver"])
        .option("dbtable", query)
        .load())
```

### 3. Escritura como JSON Lines a GCS Landing
Convertimos los datos a Pandas y los subimos a GCS estructurados en formato **JSON Lines** (un registro JSON por línea), facilitando que BigQuery los procese de manera nativa e incremental:
```python
today = datetime.datetime.today().strftime('%d%m%Y')
JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"

bucket = storage_client.bucket(GCS_BUCKET)
blob = bucket.blob(JSON_FILE_PATH)
# Convertir df a JSON Lines y subir a GCS
blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")
```

---

## Paso 6: Auditoría y Logging del Pipeline

Para tener visibilidad total del rendimiento del pipeline, se implementa un sistema de logs en memoria que se persiste al finalizar el proceso en dos destinos:

1. **Tabla de Auditoría de Cargas (`audit_log` en BigQuery):** Registra el recuento de filas y el tipo de carga procesada para que sirva de insumo al siguiente ciclo incremental.
   ```python
   audit_df = spark.createDataFrame([
       ("hospital_a_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")],
       ["data_source", "tablename", "load_type", "record_count", "load_timestamp", "status"])

   (audit_df.write.format("bigquery")
       .option("table", BQ_AUDIT_TABLE)
       .option("temporaryGcsBucket", GCS_BUCKET)
       .mode("append")
       .save())
   ```
2. **Historial de Logs del Pipeline (`pipeline_logs` en BigQuery y GCS):** Salva un histórico detallado de los eventos ocurridos (conexiones exitosas, archivos movidos, errores) en formato JSON estructurado tanto en BigQuery como en una carpeta de almacenamiento en GCS (`gs://{GCS_BUCKET}/temp/pipeline_logs/`).
   ```python
   # Guardar Logs a GCS como JSON
   save_logs_to_gcs()
   # Guardar Logs a BigQuery para analítica de performance
   save_logs_to_bigquery()
   ```
