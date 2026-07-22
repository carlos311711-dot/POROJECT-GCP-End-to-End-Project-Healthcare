# MANUAL DE CI/CD: AUTOMATIZACIÓN DE DESPLIEGUE EN EL DATALAKE
## Integración y Despliegue Continuo con GitHub, Google Cloud Build y Cloud Composer (Airflow)

Este manual detalla la **Fase 4: Integración y Despliegue Continuo (CI/CD)**. En un entorno empresarial, los ingenieros de datos no cargan archivos manualmente en la nube. En su lugar, el código desarrollado de forma local se envía a un repositorio central en **GitHub**, el cual dispara de manera automatizada una canalización de **Google Cloud Build** para validar, compilar y sincronizar todos los DAGs y scripts de datos en el bucket de **Cloud Composer** (Airflow).

---

## 📖 Tabla de Contenidos
1. [¿Por qué implementar CI/CD en Ingeniería de Datos?](#1-por-qué-implementar-cicd-en-ingeniería-de-datos)
2. [El Flujo de Trabajo del Ciclo de Vida del Software (SDLC)](#2-el-flujo-de-trabajo-del-ciclo-de-vida-del-software-sdlc)
3. [Paso 1: El Archivo de Configuración cloudbuild.yaml](#paso-1-el-archivo-de-configuración-cloudbuildyaml)
4. [Paso 2: Conexión de GitHub con Google Cloud Build](#paso-2-conexión-de-github-con-google-cloud-build)
5. [Paso 3: Configuración del Disparador (Trigger) en GCP](#paso-3-configuración-del-disparador-trigger-en-gcp)
6. [Paso 4: Operaciones de Desarrollo Diario y Trazabilidad](#paso-4-operaciones-de-desarrollo-diario-y-trazabilidad)

---

## 1. ¿Por qué implementar CI/CD en Ingeniería de Datos?

La modificación manual de archivos en consola (cargar DAGs arrastrando archivos al bucket de GCS, o actualizar scripts de Spark vía web) genera fallas operativas y falta de control:
* **Falta de Trazabilidad:** No se sabe quién modificó un pipeline o consulta SQL ni en qué momento.
* **Inconsistencia de Entornos:** El código de desarrollo no coincide con el de pruebas (QA) o producción.
* **Bloqueos y Caídas:** Subir un DAG con errores de sintaxis Python puede bloquear el planificador de Airflow completo.

> [!IMPORTANT]
> **CI/CD** resuelve estos problemas automatizando la validación de sintaxis e inyectando de forma segura y uniforme el código en los buckets correspondientes de Cloud Composer solo cuando el código ha sido aprobado y fusionado en la rama principal.

---

## 2. El Flujo de Trabajo del Ciclo de Vida del Software (SDLC)

El pipeline de CI/CD automatiza los pasos desde la máquina del desarrollador hasta la ejecución en GCP:

```
[ Máquina del Ingeniero ] ──(1. Git Push)──> [ Repositorio GitHub ]
                                                      │
                                                      │ (2. Dispara Trigger)
                                                      ▼
[ GCS Bucket (Composer) ] <──(4. Despliega)── [ Google Cloud Build ] (3. Ejecuta yaml)
```

1. **Desarrollo Local:** El ingeniero de datos clona el repositorio del proyecto en su máquina de desarrollo local y realiza las actualizaciones en los archivos SQL o scripts de Spark.
2. **Git Commit & Push:** El ingeniero envía los cambios a la rama principal de GitHub.
3. **Trigger de Cloud Build:** Google Cloud Build detecta el cambio en GitHub y activa una compilación de forma automática.
4. **Sincronización:** Cloud Build lee el archivo `cloudbuild.yaml`, ejecuta los pasos descritos y despliega los archivos modificados a los directorios del bucket de Cloud Composer.
5. **Auto-Carga en Airflow:** Cloud Composer detecta de inmediato los nuevos archivos y actualiza el servidor de Airflow en segundos de forma automática.

---

## Paso 1: El Archivo de Configuración cloudbuild.yaml

El archivo `cloudbuild.yaml` define la secuencia de pasos lógicos que ejecutará el contenedor de Cloud Build en GCP:

```yaml
steps:
  # Paso 1: Instalar dependencias del sistema y utilidades de Python
  - name: 'python:3.9-slim'
    entrypoint: 'pip'
    args: ['install', '-r', 'requirements.txt']

  # Paso 2: Sincronizar archivos DAG con la carpeta /dags/ de Cloud Composer
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        echo "Sincronizando DAGs de Airflow..."
        gsutil -m rsync -d -r workflows/ gs://us-central1-healthcare-composer-bucket/dags/

  # Paso 3: Sincronizar scripts de procesamiento y SQL con la carpeta /data/
  - name: 'gcr.io/google.com/cloudsdktool/cloud-sdk'
    entrypoint: 'bash'
    args:
      - '-c'
      - |
        echo "Sincronizando scripts SQL de BigQuery y PySpark..."
        gsutil -m rsync -d -r Scripts/ gs://us-central1-healthcare-composer-bucket/data/Scripts/
        gsutil -m rsync -d -r data/BQ/ gs://us-central1-healthcare-composer-bucket/data/BQ/

options:
  logging: CLOUD_LOGGING_ONLY
```

* **`gsutil -m rsync`:** La utilidad realiza una sincronización en paralelo (`-m`) y remueve en el destino aquellos archivos que ya no existan en el origen (`-d`), manteniendo limpio y sincronizado el entorno del Composer.

---

## Paso 2: Conexión de GitHub con Google Cloud Build

Para que GCP pueda oír los eventos de GitHub, debemos establecer una vinculación segura entre ambas cuentas:

1. En la consola de GCP, busca y abre el servicio de **Cloud Build**.
2. En el panel izquierdo de navegación, ve a **Disparadores (Triggers)**.
3. Haz clic en **Administrar Repositorios (Manage Repositories)** y presiona **Conectar Repositorio**.
4. Selecciona **GitHub (Cloud Build GitHub App)** como proveedor de origen y autentícate con tus credenciales de GitHub.
5. Selecciona tu repositorio (ej. `jhonvelasque/POROJECT-GCP-End-to-End-Project-Healthcare`) y haz clic en **Conectar**.

---

## Paso 3: Configuración del Disparador (Trigger) en GCP

El disparador es el "oyente" que ejecutará nuestro pipeline al detectar cambios de código:

1. Dentro de Cloud Build > Disparadores, haz clic en **Crear Disparador**.
2. Configura los parámetros básicos:
   - **Nombre:** `despliegue-composer-datalake`
   - **Descripción:** `Sincroniza DAGs y scripts a Cloud Composer ante cambios en GitHub`
   - **Evento:** *Push a una rama*
3. Configura el origen del código:
   - **Repositorio:** Selecciona el repositorio de GitHub previamente conectado.
   - **Rama:** `^main$` (se activará solo ante commits confirmados en la rama principal `main`).
4. Configura la construcción:
   - **Configuración:** Selecciona *Archivo de configuración de Cloud Build (yaml o json)*.
   - **Ubicación del archivo:** `cloudbuild.yaml` (alojado en la raíz del proyecto).
5. **Cuenta de Servicio:** Selecciona la cuenta de servicio de Cloud Build adecuada. Garantiza en el panel IAM de GCP que esta cuenta posea los roles:
   * **Administrador de Storage (Storage Admin):** Permite leer, escribir y borrar objetos en el bucket de GCS de Cloud Composer.
   * **Usuario de Cloud Composer (Composer User):** Permite la comunicación con el entorno Airflow.
6. Haz clic en **Crear**.

---

## Paso 4: Operaciones de Desarrollo Diario y Trazabilidad

Una vez configurada la arquitectura de CI/CD, este es el flujo operativo diario del Ingeniero de Datos:

1. **Clonar localmente:**
   ```bash
   git clone https://github.com/jhonvelasque/POROJECT-GCP-End-to-End-Project-Healthcare.git
   ```
2. **Modificar o agregar código:** Por ejemplo, actualizar una consulta en `data/BQ/bronze.sql` o añadir un nuevo DAG en la carpeta `workflows/`.
3. **Poner en staging y realizar el commit:**
   ```bash
   git add .
   git commit -m "feat: Agregar nueva tabla externa de auditoría en BigQuery"
   ```
4. **Subir los cambios (Push):**
   ```bash
   git push origin main
   ```
5. **Monitoreo automático:**
   - Al instante del push, Cloud Build iniciará la compilación. Puedes ver el estado de avance en tiempo real en la pestaña **Historial** de Cloud Build en GCP.
   - Al finalizar con éxito, el log de construcción listará los archivos sincronizados.
   - Si abres la consola de **Cloud Composer / Airflow**, verás reflejadas las modificaciones de tus DAGs y scripts de manera inmediata y sin interrupciones de servicio.
