# MANUAL DE ORQUESTACIÓN Y AUTOMATIZACIÓN: APACHE AIRFLOW & CLOUD COMPOSER
## Orquestación del Pipeline de Ingesta (PySpark/Dataproc) y Procesamiento (BigQuery)

Este manual detalla la **Fase 3: Orquestación del Pipeline** utilizando **Google Cloud Composer** (el servicio totalmente administrado de **Apache Airflow** en GCP). Se explica la arquitectura de orquestación basada en el patrón de diseño **Padre e Hijos (Parent-Child DAGs)** para automatizar la ejecución de jobs en Spark y consultas SQL en BigQuery de forma secuencial y sin intervención manual.

---

## 📖 Tabla de Contenidos
1. [Introducción a la Orquestación con Cloud Composer](#1-introducción-a-la-orquestación-con-cloud-composer)
2. [Estrategia de Diseño: Patrón de Orquestación Parent-Child](#2-estrategia-de-diseño-patrón-de-orquestación-parent-child)
3. [Paso 1: El DAG de Ingesta - PySpark (Dataproc)](#paso-1-el-dag-de-ingesta---pyspark-dataproc)
4. [Paso 2: El DAG de Base de Datos - Procesamiento (BigQuery)](#paso-2-el-dag-de-base-de-datos---procesamiento-bigquery)
5. [Paso 3: El DAG Maestro - Orquestador (Parent)](#paso-3-el-dag-maestro---orquestador-parent)
6. [Despliegue de DAGs y CI/CD en la Empresa](#6-despliegue-de-dags-y-cicd-en-la-empresa)

---

## 1. Introducción a la Orquestación con Cloud Composer

En un pipeline de ingeniería de datos empresarial, la ejecución de scripts no se hace de forma manual o aislada. El éxito de la carga analítica final depende de una secuencia lógica exacta:

```
[ Fuentes de Origen ] ──(Ingesta Spark)──> [ GCS Landing ] ──(DML BigQuery)──> [ Capas Plata y Oro ]
```

Para coordinar esto, GCP provee **Cloud Composer**, que implementa **Apache Airflow**. Permite programar, programar alarmas de error, monitorear la infraestructura y encadenar dependencias mediante flujos de trabajo definidos como código Python (**DAGs** - *Directed Acyclic Graphs*).

---

## 2. Estrategia de Diseño: Patrón de Orquestación Parent-Child

En lugar de construir un único DAG gigante y monolítico que contenga todas las tareas del clúster de Spark y del almacén de BigQuery, se adopta un diseño **modular y desacoplado**:

```mermaid
graph TD
    %% Padre
    subgraph DAG Maestro (Parent)
        MAESTRO[Master Orchestrator DAG<br/>Programación Temporal]
    end

    %% Hijos
    subgraph DAG Hijo 1: Ingestion
        SPARK_START[Iniciar Clúster Dataproc]
        JOB_A[Job Spark: Hospital A]
        JOB_B[Job Spark: Hospital B]
        JOB_C[Job Spark: Claims]
        JOB_D[Job Spark: CPT Codes]
        SPARK_STOP[Detener Clúster Dataproc]

        SPARK_START --> JOB_A & JOB_B & JOB_C & JOB_D --> SPARK_STOP
    end

    subgraph DAG Hijo 2: Processing
        BQ_BRONZE[Carga Capa Bronce]
        BQ_SILVER[Procesamiento Plata]
        BQ_GOLD[Consolidación Oro]

        BQ_BRONZE --> BQ_SILVER --> BQ_GOLD
    end

    %% Relaciones
    MAESTRO -->|1. Trigger| SPARK_START
    SPARK_STOP -->|2. Éxito Trigger| BQ_BRONZE
```

### Beneficios del Patrón Parent-Child:
* **Mantenibilidad:** Si falla un script SQL en BigQuery, podemos re-ejecutar solo la sección de base de datos sin necesidad de volver a encender el clúster de Dataproc ni repetir la ingesta JDBC desde MySQL.
* **Desacoplamiento:** Los DAGs hijos no tienen programación horaria propia (`schedule_interval=None`). El DAG Padre es el único que posee la programación de tiempo (ej. diariamente a la 1:00 AM) y activa secuencialmente a los hijos.

---

## Paso 1: El DAG de Ingesta - PySpark (Dataproc)

Este DAG (`child_pyspark_ingestion.py`) se encarga de encender la infraestructura transitoria de Dataproc, enviar los cuatro (4) trabajos de ingesta y apagar el clúster al finalizar para evitar costos redundantes:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from datetime import datetime, timedelta

# Configuración base del flujo
default_args = {
    'owner': 'GTM-Digitales',
    'start_date': datetime(2026, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'child_pyspark_ingestion',
    default_args=default_args,
    schedule_interval=None,  # Activado por el padre
    catchup=False
) as dag:

    # 1. Tarea para iniciar un clúster Dataproc existente para ahorrar recursos
    start_dataproc_cluster = DataprocStartClusterOperator(
        task_id='start_dataproc_cluster',
        project_id='avd-databricks-demo',
        region='us-central1',
        cluster_name='my-demo-cluster2'
    )

    # 2. Configuración y envío de Jobs de Spark (Ejemplo: Hospital A)
    pyspark_job_hosa = {
        'reference': {'project_id': 'avd-databricks-demo'},
        'placement': {'cluster_name': 'my-demo-cluster2'},
        'pyspark_job': {'main_python_file_uri': 'gs://composer-bucket/data/hospitalA_mysqlToLanding.py'}
    }

    submit_job_hosa = DataprocSubmitJobOperator(
        task_id='submit_job_hospital_a',
        job=pyspark_job_hosa,
        region='us-central1',
        project_id='avd-databricks-demo'
    )

    # (Se repite la configuración para Hospital B, Claims y CPT Codes)

    # 3. Tarea para apagar el clúster y frenar la facturación de Compute Engine
    stop_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='stop_dataproc_cluster',
        project_id='avd-databricks-demo',
        region='us-central1',
        cluster_name='my-demo-cluster2'
    )

    # Secuencia de dependencias
    start_dataproc_cluster >> submit_job_hosa >> stop_dataproc_cluster
```

---

## Paso 2: El DAG de Base de Datos - Procesamiento (BigQuery)

Este DAG (`child_bigquery_processing.py`) no administra infraestructura física, sino que se comunica directamente con BigQuery para orquestar la transformación de las capas del Datalake:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

# Función para leer el archivo SQL alojado en GCS o local del Composer
def read_sql_query(filepath):
    with open(filepath, 'r') as file:
        return file.read()

with DAG(
    'child_bigquery_processing',
    start_date=datetime(2026, 3, 25),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. Procesamiento Bronce (Carga de tablas externas)
    run_bronze_layer = BigQueryInsertJobOperator(
        task_id='run_bronze_layer',
        configuration={
            "query": {
                "query": read_sql_query('/home/airflow/gcs/data/bronze.sql'),
                "useLegacySql": False,
            }
        }
    )

    # 2. Procesamiento Plata (Limpieza, CDM, SCD Tipo 2)
    run_silver_layer = BigQueryInsertJobOperator(
        task_id='run_silver_layer',
        configuration={
            "query": {
                "query": read_sql_query('/home/airflow/gcs/data/silver.sql'),
                "useLegacySql": False,
            }
        }
    )

    # 3. Procesamiento Oro (Hechos y Dimensiones de Resumen)
    run_gold_layer = BigQueryInsertJobOperator(
        task_id='run_gold_layer',
        configuration={
            "query": {
                "query": read_sql_query('/home/airflow/gcs/data/gold.sql'),
                "useLegacySql": False,
            }
        }
    )

    # Secuencia lógica
    run_bronze_layer >> run_silver_layer >> run_gold_layer
```

---

## Paso 3: El DAG Maestro - Orquestador (Parent)

El orquestador maestro (`parent_orchestrator.py`) contiene la lógica de programación horaria corporativa y encadena los dos procesos hijos utilizando el operador de disparadores:

```python
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'GTM-Digitales',
    'start_date': datetime(2026, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'parent_orchestrator',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Ejecución diaria a la 1:00 AM (America/Lima)
    catchup=False
) as dag:

    # Disparar la ingesta en Spark de forma prioritaria
    trigger_pyspark = TriggerDagRunOperator(
        task_id='trigger_pyspark_ingestion',
        trigger_dag_id='child_pyspark_ingestion',
        wait_for_completion=True,  # Esperar a que el DAG hijo finalice con éxito
        poke_interval=30
    )

    # Disparar las consultas SQL en BigQuery una vez finalizada la ingesta
    trigger_bigquery = TriggerDagRunOperator(
        task_id='trigger_bigquery_processing',
        trigger_dag_id='child_bigquery_processing',
        wait_for_completion=True,
        poke_interval=30
    )

    # Dependencia global del Datalake
    trigger_pyspark >> trigger_bigquery
```

---

## 4. Despliegue de DAGs y CI/CD en la Empresa

En producción, los ingenieros de datos no cargan archivos manualmente a la interfaz de Airflow. Se utiliza un pipeline de Integración y Despliegue Continuo (**CI/CD**):

1. **Desarrollo y Commit:** El ingeniero escribe o modifica los DAGs en su entorno local y los sube a una rama de desarrollo en **GitHub**.
2. **Pipelines de Integración:** Un servicio de automatización (como GitHub Actions, Cloud Build o GitLab CI) valida la sintaxis de Python y los scripts SQL.
3. **Despliegue Automatizado a GCS:** Una vez aprobado el Pull Request en la rama `main`, el pipeline de CI/CD copia de forma automática:
   * Los archivos `.py` de los DAGs a la carpeta `/dags/` del bucket de Cloud Composer.
   * Los scripts auxiliares de transformación (`silver.sql`, `gold.sql`) y los scripts PySpark de extracción a la carpeta `/data/` del mismo bucket.
4. **Sincronización de Composer:** Cloud Composer detecta los nuevos archivos en su bucket asociado y actualiza la interfaz de Airflow en segundos de forma automática.
