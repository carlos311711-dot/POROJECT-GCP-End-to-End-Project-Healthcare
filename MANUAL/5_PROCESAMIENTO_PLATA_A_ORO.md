# MANUAL DE PROCESAMIENTO: CAPA PLATA A ORO EN BIGQUERY
## Modelado Dimensional, Casos de Uso de Negocio y Tablas de Resumen para BI

> [!NOTE]
> ### 📍 Ubicación del Código y Scripts SQL
> Los scripts SQL que ejecutan estas agregaciones analíticas de la capa Oro se ubican en:
> * **Capa Oro (Patient Summary):** [data/BQ/gold.sql](../data/BQ/gold.sql)
> * **Casos de Uso Adicionales (Proveedor y Departamento):** [data/BQ/assignment.sql](../data/BQ/assignment.sql)
>
> ### ⚙️ Cómo Ejecutar
> Estos scripts se ejecutan en BigQuery tras finalizar la carga de la Capa Plata. Puedes correrlos manualmente en la consola de BigQuery o mediante la herramienta de línea de comandos `bq`:
> ```bash
> bq query --use_legacy_sql=false < data/BQ/gold.sql
> ```

Este manual describe el flujo de procesamiento final en **BigQuery** desde la **Capa Plata** (datos limpios y unificados) hacia la **Capa Oro** (Gold/Reporting Layer). Se explica cómo transformar datos técnicos normalizados en información de alto valor de negocio mediante agregaciones, uniones complejas (`JOINs`) y modelos dimensionales listos para alimentar herramientas de visualización (BI).

---

## 📖 Tabla de Contenidos
1. [¿Qué es la Capa Oro (Gold Layer)?](#1-qué-es-la-capa-oro-gold-layer)
2. [Estrategia de Modelado: Tablas Dimensionales y de Hechos](#2-estrategia-de-modelado-tablas-dimensionales-y-de-hechos)
3. [Caso de Uso de Negocio: Tabla Resumen del Paciente](#3-caso-de-uso-de-negocio-tabla-resumen-del-paciente)
4. [Implementación SQL en BigQuery (DDL y DML)](#4-implementación-sql-en-bigquery-ddl-y-dml)
5. [Otros Casos de Uso Analíticos (Asignaciones)](#5-otros-casos-de-uso-analíticos-asignaciones)
6. [Estrategia de Carga en la Capa Oro](#6-estrategia-de-carga-en-la-capa-oro)

---

## 1. ¿Qué es la Capa Oro (Gold Layer)?

La **Capa Oro** es la capa de consumo final del datalake. A diferencia de la Capa Plata (que es técnica y mantiene la granularidad de origen), la Capa Oro se diseña **orientada estrictamente a responder preguntas de negocio y casos de uso**.

```
    ┌────────────────────────────────────────────────────────┐
    │ 1. CAPA PLATA (Cleaned & Integrated)                   │
    │ - silver.patients      - silver.encounters             │ (Tablas
    │ - silver.transactions  - silver.claims                 │  Normalizadas)
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼ Agregaciones, Uniones, Casos de Uso
    ┌────────────────────────────────────────────────────────┐
    │ 2. CAPA ORO (Reporting & BI)                           │
    │ - gold.fact_patient_summary                            │ (Tablas
    │ - gold.fact_provider_performance                       │  Consolidadas)
    └───────────────────────────┬────────────────────────────┘
                                │
                                ▼ Consultas rápidas y agregadas
    ┌────────────────────────────────────────────────────────┐
    │ 3. DASHBOARDS DE NEGOCIO                               │
    │ - Looker Studio, Power BI, Tableau                     │
    └────────────────────────────────────────────────────────┘
```

---

## 2. Estrategia de Modelado: Tablas Dimensionales y de Hechos

En la Capa Oro estructuramos la información bajo un modelo desnormalizado para maximizar el rendimiento de las herramientas de BI:

* **Tablas de Dimensiones (Dim Tables):** Describen los aspectos contextuales o demográficos de las entidades (ej. datos consolidados de pacientes activos, catálogos de especialidades médicas o listas de departamentos).
* **Tablas de Hechos (Fact Tables / Tablones de Resumen):** Almacenan métricas numéricas y claves de negocio asociadas a eventos ocurridos en el hospital (transacciones financieras, visitas de pacientes, copagos, etc.).

---

## 3. Caso de Uso de Negocio: Tabla Resumen del Paciente

### El Problema de Negocio:
El Director Financiero del hospital necesita evaluar el ciclo de ingresos a nivel de paciente. Para ello, requiere un reporte unificado que responda a preguntas complejas:
* *¿Qué paciente ingresó?*
* *¿Qué médico (proveedor) lo atendió y en qué departamento?*
* *¿Qué procedimiento se le realizó (código CPT) y cuál fue el costo de facturación?*
* *¿Cuánto dinero cubrió la aseguradora y cuánto pagó el paciente directamente (copago)?*
* *¿Cuál es el estado del reclamo del seguro (Aprobado / Rechazado)?*

### La Solución de Datos:
Los datos requeridos para este reporte están dispersos en múltiples tablas de la Capa Plata. Para construir el **Resumen del Paciente**, debemos unir 4 tablas clave aplicando filtros de calidad:

```
                  ┌───────────────────────────────┐
                  │       silver.patients         │
                  └──────────────┬────────────────┘
                                 │ JOIN
                  ┌──────────────┴────────────────┐
                  │       silver.encounters       │
                  └──────────────┬────────────────┘
                                 │ JOIN
                  ┌──────────────┴────────────────┐
                  │      silver.transactions      │
                  └──────────────┬────────────────┘
                                 │ JOIN
                  ┌──────────────┴────────────────┐
                  │        silver.claims          │
                  └───────────────────────────────┘
```

> [!IMPORTANT]
> **Garantía de Calidad:** Al realizar la unión en la capa de Oro, debemos filtrar obligatoriamente los registros corruptos o en cuarentena generados en la Capa Plata agregando la condición: `WHERE is_quarantined = false`.

---

## 4. Implementación SQL en BigQuery (DDL y DML)

A continuación se detalla el script estándar para crear la tabla analítica y cargarla mediante un proceso de **Carga Completa (Truncate & Load)**:

### A. DDL de la Tabla de Oro (`gold_patient_summary`)
```sql
CREATE OR REPLACE TABLE `avd-databricks-demo.gold_dataset.gold_patient_summary` (
  patient_key STRING OPTIONS(description="Clave sustituta única del paciente (CDM)"),
  first_name STRING OPTIONS(description="Primer nombre del paciente"),
  last_name STRING OPTIONS(description="Apellido del paciente"),
  gender STRING OPTIONS(description="Género del paciente"),
  dob DATE OPTIONS(description="Fecha de nacimiento del paciente"),
  state STRING OPTIONS(description="Estado de residencia"),
  encounter_id INT64 OPTIONS(description="Identificador único del encuentro clínico"),
  encounter_type STRING OPTIONS(description="Tipo de atención (Ambulatoria, Emergencia, Telemedicina)"),
  encounter_date DATE OPTIONS(description="Fecha en que ocurrió el encuentro clínico"),
  charge_amount NUMERIC(18, 4) OPTIONS(description="Monto total cobrado por el hospital"),
  payment_amount NUMERIC(18, 4) OPTIONS(description="Monto pagado directamente por el paciente"),
  insurance_provider STRING OPTIONS(description="Nombre de la compañía aseguradora"),
  claim_amount NUMERIC(18, 4) OPTIONS(description="Monto total reclamado a la aseguradora"),
  claim_status STRING OPTIONS(description="Estado del reclamo ante el seguro (Aprobado, Rechazado, Pendiente)"),
  fec_proceso DATETIME OPTIONS(description="Marca de tiempo del procesamiento final")
);
```

### B. DML para Carga y Transformación de Datos (`INSERT INTO ... SELECT`)
```sql
-- 1. Limpiar los datos existentes para recarga analítica completa
TRUNCATE TABLE `avd-databricks-demo.gold_dataset.gold_patient_summary`;

-- 2. Insertar registros unificados y enriquecidos de los pacientes
INSERT INTO `avd-databricks-demo.gold_dataset.gold_patient_summary`
SELECT
  p.patient_key,
  p.first_name,
  p.last_name,
  p.gender,
  p.dob,
  p.state,
  e.encounter_id,
  e.encounter_type,
  e.encounter_date,
  SAFE_CAST(t.charge_amount AS NUMERIC) AS charge_amount,
  SAFE_CAST(t.payment_amount AS NUMERIC) AS payment_amount,
  p.insurance AS insurance_provider,
  SAFE_CAST(c.claim_amount AS NUMERIC) AS claim_amount,
  IFNULL(c.claim_status, 'Sin Reclamación') AS claim_status,
  CURRENT_DATETIME('America/Lima') AS fec_proceso
FROM `avd-databricks-demo.silver_dataset.patients` AS p
-- Unir con Encuentros Clínicos
INNER JOIN `avd-databricks-demo.silver_dataset.encounters` AS e 
  ON p.patient_key = e.patient_key
-- Unir con Transacciones Financieras
LEFT JOIN `avd-databricks-demo.silver_dataset.transactions` AS t 
  ON e.encounter_id = t.encounter_id
-- Unir con Reclamaciones del Seguro (Claims)
LEFT JOIN `avd-databricks-demo.silver_dataset.claims` AS c 
  ON e.encounter_id = c.encounter_id AND p.patient_key = c.patient_key
-- Excluir registros en cuarentena para asegurar la calidad de datos de BI
WHERE p.is_quarantined = false AND e.is_quarantined = false;
```

---

## 5. Otros Casos de Uso Analíticos (Asignaciones)

Para consolidar el análisis del ciclo de ingresos, se deben implementar dos tablas adicionales en la Capa Oro:

### A. Resumen del Proveedor (Provider Performance Summary)
* **Objetivo:** Evaluar la productividad y rendimiento financiero de los médicos.
* **Lógica:** Agrupar por médico (`provider_name`, `specialty`) y calcular la cantidad de encuentros atendidos, el total de cargos facturados y el promedio de copagos recaudados.
* **Tablas involucradas:** `silver.providers`, `silver.encounters`, `silver.transactions`.

### B. Análisis del Departamento (Department Performance Analysis)
* **Objetivo:** Identificar qué especialidades y departamentos físicos del hospital tienen mayor afluencia y facturación.
* **Lógica:** Agrupar por `dep_name` y `encounter_type` para calcular métricas de concurrencia y rentabilidad de los centros de costo.
* **Tablas involucradas:** `silver.departments`, `silver.encounters`, `silver.transactions`.

---

## 6. Estrategia de Carga en la Capa Oro

Para el procesamiento en la Capa Oro, la práctica estándar recomendada es **Truncate & Load (Recarga Completa)**:
1. **Razones de diseño:** Al ser tablas muy agregadas destinadas a analistas y tomadores de decisiones, recalcular todo el historial garantiza que los reportes de meses pasados siempre reflejen correcciones o reclamaciones tardías que hayan sido aprobadas por los seguros.
2. **Eficiencia en BigQuery:** Al tratarse de tablas agregadas, el volumen físico es drásticamente menor que el de la Capa Plata (detalle transaccional), por lo que el costo computacional de un `TRUNCATE` e `INSERT` completo es bajo y ofrece un rendimiento óptimo de consulta en los tableros de control.
