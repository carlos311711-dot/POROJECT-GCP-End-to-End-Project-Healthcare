# GUÍA DE PREPARACIÓN PARA ENTREVISTAS: PROYECTO HEALTHCARE RCM
## Cómo Explicar el Pipeline End-to-End de Ingeniería de Datos en GCP

> [!NOTE]
> ### 📍 Ubicación del Código y Estructura del Repositorio
> * **Scripts de Ingesta PySpark (Python):** [Scripts/](../Scripts/)
> * **Scripts SQL de BigQuery:** [data/BQ/](../data/BQ/)
> * **DAGs de Orquestación de Airflow (Python):** [workflows/](../workflows/)
> * **Archivo de Configuración CI/CD:** [cloudbuild.yaml](../cloudbuild.yaml)

Esta guía ha sido diseñada para prepararte a responder preguntas técnicas y de negocio en entrevistas de Ingeniería de Datos, tomando como base este proyecto. Al inicio se presenta un mapeo estructurado para "amarrar" cada etapa teórica con su manual técnico de implementación correspondiente.

---

## 🗺️ Mapeo de Documentación Técnica (Mapeo de Referencias)

Para profundizar en el código, configuraciones y lógica detallada de cada paso expuesto en esta guía, consulta los siguientes manuales en la carpeta `MANUAL`:

1. **1. FUENTES DE DATOS (Visión General y Negocio RCM):**
   * 📄 [1_FUENTES_DE_DATOS.md](./1_FUENTES_DE_DATOS.md)
2. **2. CONFIGURACIÓN DE FUENTES DE DATOS (MySQL, GCS Buckets y Metadatos):**
   * 📄 [2_CONFIGURACION_FUENTES_DATOS.md](./2_CONFIGURACION_FUENTES_DATOS.md)
3. **3. INGESTA DE DATOS (Clústeres de Dataproc y PySpark):**
   * 📄 [3_INGESTA_DATOS_DATAPROC.md](./3_INGESTA_DATOS_DATAPROC.md)
4. **4. PROCESAMIENTO BRONCE A PLATA (Limpieza, CDM, Cuarentena e Hilos de SCD Tipo 2):**
   * 📄 [4_PROCESAMIENTO_BRONCE_A_PLATA.md](./4_PROCESAMIENTO_BRONCE_A_PLATA.md)
5. **5. PROCESAMIENTO PLATA A ORO (Tablas de Resumen, Hechos y Dimensiones para BI):**
   * 📄 [5_PROCESAMIENTO_PLATA_A_ORO.md](./5_PROCESAMIENTO_PLATA_A_ORO.md)
6. **6. ORQUESTACIÓN DE WORKFLOWS (Composer, Airflow y Secuencia Parent-Child):**
   * 📄 [6_ORQUESTACION_WORKFLOW_AIRFLOW.md](./6_ORQUESTACION_WORKFLOW_AIRFLOW.md)
7. **7. INTEGRACIÓN Y DESPLIEGUE CONTINUO (CI/CD con GitHub y Cloud Build):**
   * 📄 [7_CICD_GITHUB_CLOUDBUILD_AIRFLOW.md](./7_CICD_GITHUB_CLOUDBUILD_AIRFLOW.md)
8. **8. MODELO ENTIDAD-RELACIÓN DE CAPA ORO:**
   * 📄 [8_DIAGRAMA_ER_GOLD.md](./8_DIAGRAMA_ER_GOLD.md)
9. **9. GUÍA DE ENTREVISTAS (Este documento):**
   * 📄 [9_GUIA_PREPARACION_ENTREVISTAS.md](./9_GUIA_PREPARACION_ENTREVISTAS.md)
10. **10. VALIDACIÓN EN AIRFLOW (Hola Mundo en Cloud Composer):**
    * 📄 [10_HOLA_MUNDO_AIRFLOW.md](./10_HOLA_MUNDO_AIRFLOW.md)

---

## 1. Resumen del Proyecto (Elevator Pitch de 30 Segundos)

> "Diseñé e implementé un pipeline de datos de extremo a extremo en **Google Cloud Platform (GCP)** para optimizar la **Gestión del Ciclo de Ingresos Sanitarios (RCM)**. 
>
> Extraje datos operacionales EMR (Pacientes, Citas, Médicos) desde servidores de base de datos distribuidos en Cloud SQL utilizando clústeres de **Dataproc (PySpark)** configurados por metadatos. Estos datos, junto a CSVs mensuales de aseguradoras (Claims) y catálogos estáticos (CPT), se centralizan en la capa Landing de **Google Cloud Storage (GCS)**. 
>
> Posteriormente, los estructuré en **BigQuery** bajo una arquitectura Medallón, aplicando limpiezas de datos, unificación en un Modelo de Datos Común (CDM) y control histórico mediante Slowly Changing Dimensions (SCD Tipo 2) con sentencias MERGE, haciéndolos consumibles y 100% fiables para reportes analíticos e indicadores clave (KPI) de negocio. Todo el proceso está orquestado con **Cloud Composer (Airflow)** e integrado con canalizaciones automatizadas de **CI/CD** usando GitHub y Cloud Build."

---

## 2. Explicación del Problema de Negocio

El sector salud enfrenta problemas severos en el flujo del ciclo financiero debido a la fragmentación de la información de sus pacientes y visitas (Encounters). 

### Puntos clave a explicar:
* **Fragmentación de Fuentes:** Los hospitales de una misma cadena suelen contar con diferentes bases de datos transaccionales de EMR. Sus identificadores internos colisionan y sus estructuras de tablas difieren.
* **Consistencia e Integración Financiera:** Para evaluar la salud económica del hospital, se requiere cruzar los datos demográficos del paciente con sus encuentros clínicos, los cobros del hospital (Transactions) y la respuesta final de las compañías de seguros (Claims), la cual llega de forma desestructurada en archivos planos mensuales.
* **Calidad de Datos y Privacidad:** Al tratar con datos sensibles e historiales médicos, se requiere validar rigurosamente la calidad de la información (descartando nulos y duplicados lógicos) y estructurar arquitecturas robustas que controlen la trazabilidad sin pérdida de datos.

---

## 3. Caminata por la Arquitectura (Data Walkthrough)

Al explicar la arquitectura al entrevistador, llévalo en orden secuencial (Fases 1 a 4):

1. **Fase de Ingesta y Aterrizaje:** Extracción mediante **PySpark JDBC** conectándose a bases de datos operacionales en Cloud SQL. Los datos se suben a GCS como JSON Lines, archivando previamente los JSON del día anterior en carpetas estructuradas por fecha para mantener limpia la zona de aterrizaje.
2. **Capa Bronce (BigQuery):** Modelado a través de **Tablas Externas** de BigQuery que apuntan directamente a GCS. Esto optimiza el costo del datalake evitando almacenamiento duplicado inicial.
3. **Capa Plata (BigQuery):** Aquí ocurre la magia de la integración. Aplicamos un **Modelo de Datos Común (Surrogate Keys)** para evitar colisiones de llaves entre hospitales, un flag de **Cuarentena** para aislar registros incompletos sin frenar el proceso, y **SCD Tipo 2** vía comandos `MERGE` de SQL para registrar la historia de cambios del paciente.
4. **Capa Oro (BigQuery) y Orquestación:** Creación de tablones consolidados desnormalizados (como el *Patient Summary*) listos para su uso directo en tableros de control de BI. Todo automatizado de extremo a extremo mediante **Cloud Composer (Airflow)** y **Cloud Build / GitHub** para el despliegue automático del código.

---

## 4. Puntos Técnicos a Destacar (Qué te hará destacar)

* **Enfoque Orientado a Metadatos:** Explica que el pipeline de PySpark no tiene tablas quemadas en código duro. Se lee un archivo de configuración en GCS (`load_config.csv`) que define de forma dinámica qué tablas están activas y si la extracción debe ser **Incremental** (basada en marcas de tiempo y auditorías) o **Completa (Full)**.
* **Manejo Lógico de Calidad (Cuarentenas):** Destaca que en lugar de descartar filas con nombres o IDs vacíos y alterar las métricas de negocio, los marcas con un flag `is_quarantined = true` mediante expresiones `CASE WHEN`. Esto mantiene íntegra la tabla de Plata pero permite al analista aislar los datos malos.
* **Arquitectura Transitoria en Spark:** Comenta que para optimizar costos de computación, el DAG de Airflow enciende el clúster de Dataproc para procesar la ingesta diaria de PySpark y lo apaga inmediatamente al finalizar.
* **Patrón de Diseño de Orquestación Parent-Child:** Explica cómo utilizaste el operador de Airflow `TriggerDagRunOperator` para desacoplar el pipeline de ingesta de la infraestructura (Spark) de las transformaciones lógicas en base de datos (BigQuery), facilitando el soporte y mantenimiento.

---

## 5. Preguntas Comunes en Entrevistas y Respuestas Sugeridas

### P: ¿Cómo gestionan la calidad de datos y qué ocurre con los registros nulos?
> **R:** *"A nivel de la capa de transformación en BigQuery, implementamos un control de calidad. Si un campo requerido por negocio (como el ID del paciente, nombres o fecha de nacimiento) viene nulo, el registro se marca con un flag booleano `is_quarantined = true` mediante expresiones `CASE WHEN`. En la capa final Oro (consumo para dashboards), filtramos estas filas usando `WHERE is_quarantined = false` para garantizar que la reportería sea limpia, pero conservamos los datos corruptos en la capa Plata para auditorías de origen."*

### P: ¿Cómo evitan colisiones de IDs cuando unifican datos de varios hospitales?
> **R:** *"Creamos un Modelo de Datos Común (CDM). Como cada hospital genera sus propios IDs numéricos que pueden solaparse, creamos llaves sustitutas unificadas en la tabla de Plata (ej. `patient_key`). Concatenamos el ID operacional con el nombre abreviado de la sucursal de origen (`CONCAT(source_system, '-', patient_id)`). Esto garantiza claves únicas en todo el Datalake."*

### P: ¿Por qué decidieron utilizar Truncate & Load en la capa Oro en lugar de cargas incrementales?
> **R:** *"Las tablas agregadas de la capa Oro son de un volumen significativamente menor comparado con el detalle transaccional de Plata. Usar Truncate & Load diario garantiza que si hay actualizaciones tardías en el estado de una reclamación de seguro de meses anteriores, la información se recalcule correctamente reflejando el estado real en los dashboards analíticos, siendo computacionalmente muy eficiente en BigQuery."*

### P: ¿Cómo aprovisionaron y configuraron Apache Airflow en la nube de GCP?
> **R:** *"Utilizamos Google Cloud Composer 2 bajo una escala 'Small' para optimizar costos de desarrollo y pruebas. Configuramos una cuenta de servicio dedicada con los roles mínimos necesarios (Composer Worker, Dataproc Editor, Storage Admin y BigQuery Admin). Esto nos permite orquestar de forma segura todo el pipeline, permitiendo a los workers de Airflow encender el clúster de Dataproc, enviar jobs de PySpark, y ejecutar los scripts SQL de DDL y DML en BigQuery sin otorgar privilegios excesivos ni de superusuario."*

---

## 6. Formato de Narración STAR (Caso Real de Reto de Ingeniería)

* **Situación (S):** Durante las primeras pruebas del pipeline de ingesta operando cargas incrementales en Spark, nos enfrentamos a problemas de consumo excesivo de memoria RAM en el clúster de Dataproc. Esto provocaba que el pipeline fallara de forma intermitente debido a errores de tipo `OutOfMemory` del driver de Spark.
* **Tarea (T):** Tenía que optimizar el uso de recursos y memoria en el pipeline de ingesta sin incrementar los límites de la máquina en Compute Engine para mantener los costos acotados.
* **Acción (A):** Analicé el script de PySpark y detecté que se estaba forzando una conversión pesada de DataFrame a Pandas (`toPandas().to_json()`) para escribir el formato JSON Lines en GCS de forma consecutiva dentro de un bucle `for` de tablas. Rediseñé el flujo implementando un mecanismo de archivado histórico en GCS antes de la extracción y configuré el clúster de Dataproc para ser transitorio y transaccional por tabla, limpiando las sesiones y la caché de memoria de Spark entre cada iteración del bucle.
* **Resultado (R):** El tiempo de ejecución global se redujo un 30% y se eliminaron por completo las caídas por falta de memoria. Esto permitió mantener máquinas económicas y estables en la nube de GCP, asegurando un pipeline confiable y automatizado.

---

## 7. Lista Final de Comprobación Antes de Entrar a la Entrevista

* [ ] ¿Puedo explicar detalladamente en qué consiste el ciclo de ingresos (RCM) en el sector salud?
* [ ] ¿Tengo claro cómo se relaciona el modelo común de datos (CDM) y las llaves sustitutas con la resolución de colisiones de IDs?
* [ ] ¿Sé explicar el flujo lógico de uniones SQL que componen el *Patient Summary* de la capa Oro?
* [ ] ¿Puedo detallar la lógica de la sentencia `MERGE` para ejecutar actualizaciones e inserciones históricas (SCD Tipo 2)?
* [ ] ¿Sé argumentar por qué se utilizan clústeres transitorios en Dataproc y el beneficio de la orquestación Parent-Child en Airflow?
* [ ] ¿Puedo describir cómo se despliega y actualiza de forma automática el pipeline usando GitHub y Cloud Build?
