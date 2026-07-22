# GUÍA PASO A PASO: CONFIGURACIÓN DE FUENTES DE DATOS E INFRAESTRUCTURA
## Replicación del Entorno Operacional del Sector Salud en GCP (SQL DBs, GCS, BQ, Configs)

En entornos corporativos reales, las fuentes de datos operacionales de los hospitales ya existen y están en producción. Para replicar este escenario del mundo real con fines de desarrollo y pruebas, este manual describe paso a paso cómo configurar la infraestructura inicial en **Google Cloud Platform (GCP)** y cargar los datos de prueba.

---

## 📖 Tabla de Contenivos
1. [Paso 1: Configuración de Cloud SQL (MySQL)](#paso-1-configuración-de-cloud-sql-mysql)
2. [Paso 2: Creación del Modelo de Datos EMR (DDL SQL)](#paso-2-creación-del-modelo-de-datos-emr-ddl-sql)
3. [Paso 3: Creación y Estructura del Bucket en Google Cloud Storage (GCS)](#paso-3-creación-y-estructura-del-bucket-en-google-cloud-storage-gcs)
4. [Paso 4: Importación de Datos de Muestra a Cloud SQL](#paso-4-importación-de-datos-de-muestra-a-cloud-sql)
5. [Paso 5: Configuración del Enfoque Basado en Metadatos (load_config.csv)](#paso-5-configuración-del-enfoque-basado-en-metadatos-load_configcsv)
6. [Paso 6: Carga de Archivos de Claims y Códigos CPT](#paso-6-carga-de-archivos-de-claims-y-códigos-cpt)

---

## Paso 1: Configuración de Cloud SQL (MySQL)

Utilizaremos **Google Cloud SQL** para hospedar las bases de datos de historias clínicas electrónicas (EMR) de dos sucursales del hospital.

### A. Crear la Instancia para el Hospital A
1. Ve a la consola de GCP y hace clic en **Cloud SQL**.
2. Haz clic en **Crear Instancia** y selecciona el motor de base de datos **MySQL**.
3. Configura los siguientes parámetros:
   - **ID de la instancia:** `hospital-a`
   - **Contraseña del usuario root:** `12345`
   - **Database Version:** MySQL 8.0
   - **Tipo de entorno (Edición):** Enterprise - Desarrollo (Development).
   - **Región:** `us-central1` (EE. UU. Central 1).
   - **Machine Configuration:** 2 vCPU, 8 GB RAM.
   - **Storage:** 20 GB SSD.
4. Desactiva temporalmente las opciones de *Protección contra eliminación de instancias* (solo para propósitos de prueba/desarrollo) y haz clic en **Crear Instancia**.

### B. Crear la Instancia para el Hospital B
Repite exactamente los mismos pasos descritos para el Hospital A, modificando únicamente el identificador:
- **ID de la instancia:** `hospital-b`
- **Contraseña del usuario root:** `12345`

### C. Habilitar el Acceso de Red (IP Pública)
Para permitir que nuestra máquina de desarrollo local o el clúster de Dataproc accedan a Cloud SQL sin restricciones de red complejas durante el laboratorio:
1. Entra a la instancia creada (`hospital-a` o `hospital-b`).
2. En el panel izquierdo, haz clic en **Conexiones (Connections)**.
3. Ve a la pestaña **Red (Networking)**.
4. Ve a Redes autorizadas -->Haz clic en **Añadir Red (Add Network)** y configura:
   - **Nombre:** `Acceso General`
   - **Red:** `0.0.0.0/0`
5. Haz clic en **Guardar**.

> [!WARNING]
> Habilitar `0.0.0.0/0` abre la base de datos a internet y solo debe usarse en entornos de aprendizaje con contraseñas seguras y datos ficticios. En producción, se debe añadir estrictamente el rango IP de la VPN de la empresa o conectar las instancias mediante Private Service Connect (IP Privada) en la VPC.

### D. Crear Base de Datos y Usuarios Operacionales
Una vez que las instancias estén listas, crea las bases de datos lógicas y el usuario que usará Spark para la extracción JDBC:

1. **Creación de la Base de Datos:**
   - En la sección **Bases de datos** de la instancia, haz clic en **Crear Base de Datos**.
   - Nombra la base de datos como `hospital_a_db` (para la instancia A) y `hospital_b_db` (para la instancia B).
2. **Creación de Usuario Operacional:**
   - Ve a la sección **Usuarios** y haz clic en **Añadir cuenta de usuario**.
   - Define:
     - **Nombre de usuario:** `myuser`
     - **Contraseña:** `mypass`
     - **Host:** `%` (Permitir conexión desde cualquier host).

---

## Paso 2: Creación del Modelo de Datos EMR (DDL SQL)

Conéctate a la instancia mediante **Cloud SQL Studio** usando las credenciales del usuario creado (`myuser` / `mypass`) y ejecuta el siguiente script DDL para crear las 5 tablas que representan el historial médico electrónico de los pacientes:

```sql
-- Crear tabla de Departamentos
CREATE TABLE IF NOT EXISTS departments (
  dep_id INT PRIMARY KEY,
  dep_name VARCHAR(100) NOT NULL
);

-- Crear tabla de Proveedores (Médicos)
CREATE TABLE IF NOT EXISTS providers (
  provider_id INT PRIMARY KEY,
  provider_name VARCHAR(100) NOT NULL,
  npi VARCHAR(10) NOT NULL,
  specialty VARCHAR(100) NOT NULL
);

-- Crear tabla de Pacientes
CREATE TABLE IF NOT EXISTS patients (
  patient_id INT PRIMARY KEY,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  dob DATE NOT NULL,
  gender VARCHAR(10) NOT NULL,
  state VARCHAR(20) NOT NULL,
  insurance VARCHAR(50) NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Crear tabla de Encuentros (Visitas)
CREATE TABLE IF NOT EXISTS encounters (
  encounter_id INT PRIMARY KEY,
  patient_id INT NOT NULL,
  provider_id INT NOT NULL,
  department_id INT NOT NULL,
  encounter_date DATE NOT NULL,
  encounter_type VARCHAR(50) NOT NULL,
  cpt_code VARCHAR(5) NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
  FOREIGN KEY (provider_id) REFERENCES providers(provider_id),
  FOREIGN KEY (department_id) REFERENCES departments(dep_id)
);

-- Crear tabla de Transacciones Financieras
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id INT PRIMARY KEY,
  encounter_id INT NOT NULL,
  charge_amount DECIMAL(10, 2) NOT NULL,
  payment_amount DECIMAL(10, 2) NOT NULL,
  transaction_date DATE NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (encounter_id) REFERENCES encounters(encounter_id)
);
```

> [!NOTE]
> Ejecuta este script DDL tanto en la base de datos `hospital_a_db` (Hospital A) como en `hospital_b_db` (Hospital B). Ambos centros médicos comparten la misma estructura lógica relacional.

---

## Paso 3: Creación y Estructura del Bucket en Google Cloud Storage (GCS)

Google Cloud Storage actuará como nuestra zona de almacenamiento intermedio (Data Lake).

1. Ve a **Google Cloud Storage** y haz clic en **Crear Bucket**.
2. Nombra tu bucket de forma única a nivel mundial, por ejemplo: `healthcare-bucket-22032025`.
3. Selecciona una región única (`us-central1`) y tipo de almacenamiento estándar.
4. Crea la siguiente jerarquía de carpetas dentro del bucket:

```text
healthcare-bucket-22032025/
├── configs/            <-- Almacenará los metadatos de configuración ETL
├── data/               <-- Contiene los archivos CSV temporales para cargar a SQL
│   ├── hospital/       <-- Archivos CSV del Hospital A
│   └── hospitalB/      <-- Archivos CSV del Hospital B
├── landing/            <-- Capa de aterrizaje de producción de los pipelines
│   ├── hospital-a/     <-- JSON extraídos del Hospital A
│   ├── hospital-b/     <-- JSON extraídos del Hospital B
│   ├── claims/         <-- CSV mensuales de reclamaciones a aseguradoras
│   └── cptcodes/       <-- CSV estático de códigos CPT
└── temp/               <-- Ubicación temporal para procesos Spark y BigQuery
```

---

## Paso 4: Importación de Datos de Muestra a Cloud SQL

Para simular un sistema en uso real con registros históricos, importaremos datos de prueba desde archivos CSV hacia nuestras instancias de MySQL:

1. Sube los archivos CSV de muestra provistos en el repositorio a sus carpetas correspondientes en GCS:
   - Sube los archivos de Hospital A a `data/hospital/`
   - Sube los archivos de Hospital B a `data/hospitalB/`
2. Ve a la consola de **Cloud SQL**, selecciona la instancia `hospital-a` y haz clic en **Importar**.
3. Configura los parámetros de importación:
   - **Ruta de origen de GCS:** Selecciona el archivo correspondiente en el bucket (ej. `gs://healthcare-bucket-22032025/data/hospital/departments.csv`).
   - **Formato:** CSV.
   - **Base de datos:** `hospital_a_db`.
   - **Tabla de destino:** `departments`.
4. Haz clic en **Importar** y repite el proceso para las tablas `encounters`, `patients`, `providers`, y `transactions`.
5. Realiza la misma carga de datos para la instancia del **Hospital B** utilizando los archivos CSV almacenados bajo la ruta `data/hospitalB/`.

*Una vez concluido, puedes verificar la correcta importación ejecutando una consulta simple en Cloud SQL Studio:*
```sql
SELECT * FROM patients LIMIT 5;
```

---

## Paso 5: Configuración del Control por Metadatos (load_config.csv)

El pipeline de extracción se rige por un enfoque basado en metadatos. En lugar de cablear en el código PySpark qué tablas cargar de forma completa o incremental, usamos un archivo CSV de control almacenado en `gs://healthcare-bucket-22032025/configs/load_config.csv`.

### Contenido Estándar de `load_config.csv`:
```csv
datasource,src_schema,tablename,load_type,watermark_column,is_active,targetpath
hospital_a_db,hospital_a_db,departments,full,NA,1,landing/hospital-a/departments/
hospital_a_db,hospital_a_db,providers,full,NA,1,landing/hospital-a/providers/
hospital_a_db,hospital_a_db,patients,incremental,updated_at,1,landing/hospital-a/patients/
hospital_a_db,hospital_a_db,encounters,incremental,updated_at,1,landing/hospital-a/encounters/
hospital_a_db,hospital_a_db,transactions,incremental,updated_at,1,landing/hospital-a/transactions/
hospital_b_db,hospital_b_db,departments,full,NA,1,landing/hospital-b/departments/
hospital_b_db,hospital_b_db,providers,full,NA,1,landing/hospital-b/providers/
hospital_b_db,hospital_b_db,patients,incremental,updated_at,1,landing/hospital-b/patients/
hospital_b_db,hospital_b_db,encounters,incremental,updated_at,1,landing/hospital-b/encounters/
hospital_b_db,hospital_b_db,transactions,incremental,updated_at,1,landing/hospital-b/transactions/
```

### Explicación de Columnas de Configuración:
* **datasource:** Identifica el motor origen de la base de datos.
* **tablename:** Nombre de la tabla física en MySQL.
* **load_type:** 
  - `full`: Carga completa. Se extrae toda la tabla diariamente ya que los datos cambian poco (como médicos o áreas).
  - `incremental`: Carga incremental. Solo se extraen registros modificados o creados recientemente.
* **watermark_column:** Campo fecha de control (`updated_at`) que permite identificar qué registros son nuevos a partir de la última extracción.
* **is_active:** Flag de control (`1` = activo, `0` = inactivo). Si se define en `0`, el pipeline omitirá la tabla de forma automática sin necesidad de modificar el código de los scripts Spark.

---

## Paso 6: Carga de Archivos de Claims y Códigos CPT

Adicionalmente a los datos relacionales de los hospitales (EMR), el negocio requiere procesar reclamaciones a aseguradoras y catálogos estandarizados de procedimientos médicos:

1. **Reclamaciones (Claims):**
   - El cliente de negocio genera archivos CSV mensuales de reclamaciones.
   - Sube estos archivos manualmente (o vía SFTP automatizado) en la carpeta de aterrizaje:
     `gs://healthcare-bucket-22032025/landing/claims/` (ej. `claims_hospital1_june2026.csv` y `claims_hospital2_june2026.csv`).
2. **Catálogo de Códigos CPT:**
   - Sube el archivo CSV estático de correspondencia de códigos médicos y clínicos a la ruta:
     `gs://healthcare-bucket-22032025/landing/cptcodes/cptcodes.csv`.
   - Este archivo no varía a menos que se publique una nueva versión oficial de terminologías médicas.
