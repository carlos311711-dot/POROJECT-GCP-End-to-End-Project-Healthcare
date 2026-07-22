# MANUAL DE VALIDACIÓN: HOLA MUNDO EN CLOUD COMPOSER (AIRFLOW GESTIONADO)
## Despliegue y Ejecución del Primer DAG de Prueba en tu Instancia "demo-instance"

Este manual te guiará paso a paso para subir, configurar y ejecutar tu primer pipeline de datos de prueba ("Hola Mundo") en tu entorno activo de **Google Cloud Composer 2** (`demo-instance`, en la región `us-central1`).

---

## 📍 Ubicación del Código del DAG
* **Ruta Relativa del DAG en el proyecto:** [workflows/hello_world_dag.py](../workflows/hello_world_dag.py)

---

## 📖 Guía Paso a Paso para el Despliegue

### Paso 1: Encontrar el Bucket de Cloud Storage de tu Entorno
Cuando creas una instancia de Cloud Composer, Google Cloud le asocia un bucket dedicado en **Cloud Storage (GCS)**. Los archivos Python del DAG deben subirse a la carpeta `/dags/` de este bucket para que Airflow los reconozca.

1. Ve a la consola web de GCP y busca **Composer**.
2. Verás tu entorno `demo-instance` activo en la lista.
3. En la columna **Carpeta DAGs**, haz clic en el enlace. Este te redirigirá directamente a la carpeta `/dags/` del bucket en GCS (ej. `gs://us-central1-demo-instance-xxxx-bucket/dags`).
4. Toma nota del nombre del bucket (ej. `us-central1-demo-instance-xxxx-bucket`).

---

### Paso 2: Subir el DAG al Bucket de Composer

#### Opción A: Desde la Consola Web de GCP (Sin instalar nada)
1. En la pestaña del navegador de la carpeta de GCS abierta en el Paso 1, haz clic en el botón **Subir archivos**.
2. Selecciona el archivo local [hello_world_dag.py](../workflows/hello_world_dag.py) de tu computadora.
3. Espera a que la carga finalice.

#### Opción B: Mediante la CLI de Google Cloud (gcloud)
Si tienes el SDK de Google Cloud configurado en tu terminal local, puedes realizar la carga con un solo comando:
```bash
gsutil cp workflows/hello_world_dag.py gs://[REEMPLAZAR_CON_TU_BUCKET_NAME]/dags/
```

---

### Paso 3: Monitorear y Ejecutar el DAG en la Interfaz de Airflow

1. Regresa a la consola de **Composer**.
2. En la fila de tu entorno `demo-instance`, haz clic en el enlace de la columna **Airflow** o **DAG** para abrir la interfaz web de Apache Airflow.
3. **Espera de 1 a 2 minutos:** Airflow lee periódicamente el bucket de GCS. Una vez procesado el archivo, verás aparecer el DAG **`hello_world_dag`** en el panel principal.
4. **Activa el DAG:** Haz clic en el interruptor deslizante de **Off** a **On** que está a la izquierda del nombre del DAG.
5. **Ejecuta el DAG:** Haz clic en el botón **Play (Trigger DAG)** en el extremo derecho de la fila para iniciar una ejecución manual.

---

### Paso 4: Visualizar los Logs de Ejecución
1. Haz clic sobre el nombre del DAG **`hello_world_dag`** para entrar al detalle.
2. Ve a la pestaña **Grid** o **Graph**.
3. Verás las dos tareas consecutivas: `saludo_bash` y `saludo_python`.
4. Haz clic en la tarea `saludo_python`, ve a la pestaña **Log** y comprueba el mensaje registrado en el proceso:

```text
*** Reading local file: /home/airflow/gcs/logs/hello_world_dag/saludo_python/...
[2026-07-22, 16:55:00 UTC] {hello_world_dag.py:21} INFO - =========================================
[2026-07-22, 16:55:00 UTC] {hello_world_dag.py:22} INFO - ¡Hola Mundo desde PythonOperator en Cloud Composer!
[2026-07-22, 16:55:00 UTC] {hello_world_dag.py:23} INFO - =========================================
```
¡Felicidades! Tu entorno de Airflow gestionado en GCP funciona correctamente y está listo para orquestar los pipelines de datos del Datalake.
