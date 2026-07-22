--1. Monto total facturado por proveedor y departamento
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.provider_charge_summary` (
    Provider_Name STRING,
    Dept_Name STRING,
    Amount FLOAT64
);

# truncar la tabla
TRUNCATE TABLE `avd-databricks-demo.gold_dataset.provider_charge_summary`;

# insertar los datos
INSERT INTO `avd-databricks-demo.gold_dataset.provider_charge_summary`
SELECT
    CONCAT(p.firstname, ' ', p.LastName) AS Provider_Name,
    d.Name AS Dept_Name,
    SUM(t.Amount) AS Amount
FROM `avd-databricks-demo.silver_dataset.transactions` t
LEFT JOIN `avd-databricks-demo.silver_dataset.providers` p
    ON SPLIT(p.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `avd-databricks-demo.silver_dataset.departments` d
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = p.DeptID
WHERE t.is_quarantined = FALSE AND d.Name IS NOT NULL
GROUP BY Provider_Name, Dept_Name;


--------------------------------------------------------------------------------------------------
--2. Historial del Paciente (Gold): esta tabla ofrece un historial completo de las visitas, diagnósticos e interacciones financieras de un paciente.

# CREAR TABLA
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.patient_history` (
    Patient_Key STRING,
    FirstName STRING,
    LastName STRING,
    Gender STRING,
    DOB INT64,
    Address STRING,
    EncounterDate INT64,
    EncounterType STRING,
    Transaction_Key STRING,
    VisitDate INT64,
    ServiceDate INT64,
    BilledAmount FLOAT64,
    PaidAmount FLOAT64,
    ClaimStatus STRING,
    ClaimAmount STRING,
    ClaimPaidAmount STRING,
    PayorType STRING
);


# TRUNCAR TABLA
TRUNCATE TABLE `avd-databricks-demo.gold_dataset.patient_history`;

# INSERTAR DATOS
INSERT INTO `avd-databricks-demo.gold_dataset.patient_history`
SELECT
    p.Patient_Key,
    p.FirstName,
    p.LastName,
    p.Gender,
    p.DOB,
    p.Address,
    e.EncounterDate,
    e.EncounterType,
    t.Transaction_Key,
    t.VisitDate,
    t.ServiceDate,
    t.Amount AS BilledAmount,
    t.PaidAmount,
    c.ClaimStatus,
    c.ClaimAmount,
    c.PaidAmount AS ClaimPaidAmount,
    c.PayorType
FROM `avd-databricks-demo.silver_dataset.patients` p
LEFT JOIN `avd-databricks-demo.silver_dataset.encounters` e
    ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = e.PatientID
LEFT JOIN `avd-databricks-demo.silver_dataset.transactions` t
    ON SPLIT(p.Patient_Key, '-')[OFFSET(0)] || '-' || SPLIT(p.Patient_Key, '-')[OFFSET(1)] = t.PatientID
LEFT JOIN `avd-databricks-demo.silver_dataset.claims` c
    ON t.SRC_TransactionID = c.TransactionID
WHERE p.is_current = TRUE;


--------------------------------------------------------------------------------------------------
-- 3. Resumen de Desempeño del Proveedor (Gold): resume la actividad del proveedor, incluyendo el número de encuentros, el monto total facturado y la tasa de éxito de reclamos.

# CREAR TABLA
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.provider_performance` (
    ProviderID STRING,
    FirstName STRING,
    LastName STRING,
    Specialization STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    ApprovedClaims INT64,
    TotalClaims INT64,
    ClaimApprovalRate FLOAT64
);

# TRUNCAR TABLA
TRUNCATE TABLE `avd-databricks-demo.gold_dataset.provider_performance`;

# INSERTAR DATOS
INSERT INTO `avd-databricks-demo.gold_dataset.provider_performance`
SELECT
    pr.ProviderID,
    pr.FirstName,
    pr.LastName,
    pr.Specialization,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate
FROM `avd-databricks-demo.silver_dataset.providers` pr
LEFT JOIN `avd-databricks-demo.silver_dataset.encounters` e
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = e.ProviderID
LEFT JOIN `avd-databricks-demo.silver_dataset.transactions` t
    ON SPLIT(pr.ProviderID, "-")[SAFE_OFFSET(1)] = t.ProviderID
LEFT JOIN `avd-databricks-demo.silver_dataset.claims` c
    ON t.SRC_TransactionID = c.TransactionID
GROUP BY pr.ProviderID, pr.FirstName, pr.LastName, pr.Specialization;

--------------------------------------------------------------------------------------------------
-- 4. Análisis de Desempeño del Departamento (Gold): brinda visibilidad sobre la eficiencia, los ingresos y el volumen de pacientes a nivel de departamento.

# CREAR TABLA
CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.department_performance` (
    Dept_Id STRING,
    DepartmentName STRING,
    TotalEncounters INT64,
    TotalTransactions INT64,
    TotalBilledAmount FLOAT64,
    TotalPaidAmount FLOAT64,
    AvgPaymentPerTransaction FLOAT64
);

# TRUNCAR TABLA
TRUNCATE TABLE `avd-databricks-demo.gold_dataset.department_performance`;

# INSERTAR DATOS
INSERT INTO `avd-databricks-demo.gold_dataset.department_performance`
SELECT
    d.Dept_Id,
    d.Name AS DepartmentName,
    COUNT(DISTINCT e.Encounter_Key) AS TotalEncounters,
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    AVG(t.PaidAmount) AS AvgPaymentPerTransaction
FROM `avd-databricks-demo.silver_dataset.departments` d
LEFT JOIN `avd-databricks-demo.silver_dataset.encounters` e
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = e.DepartmentID
LEFT JOIN `avd-databricks-demo.silver_dataset.transactions` t
    ON SPLIT(d.Dept_Id, "-")[SAFE_OFFSET(0)] = t.DeptID
WHERE d.is_quarantined = FALSE
GROUP BY d.Dept_Id, d.Name;

--------------------------------------------------------------------------------------------------

-- 5. Métricas Financieras (Gold): agrega los KPI financieros, como los ingresos totales, la tasa de éxito de reclamos y los saldos pendientes.
-- 6. Desempeño de Pagadores y Resumen de Reclamos (Gold): esta tabla monitorea el desempeño de las aseguradoras (pagadores), enfocándose en las tasas de aprobación de reclamos, los montos pagados y la eficiencia del procesamiento.
