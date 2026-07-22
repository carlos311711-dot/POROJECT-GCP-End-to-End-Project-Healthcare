-- 4. Métricas Financieras (Gold): agrega los KPI financieros, como los ingresos totales, la tasa de éxito de reclamos y los saldos pendientes.

CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.financial_metrics` AS
SELECT
    COUNT(DISTINCT t.Transaction_Key) AS TotalTransactions,
    SUM(t.Amount) AS TotalBilledAmount,
    SUM(t.PaidAmount) AS TotalPaidAmount,
    SUM(t.Amount) - SUM(t.PaidAmount) AS OutstandingBalance,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate
FROM `avd-databricks-demo.silver_dataset.transactions` t
LEFT JOIN `avd-databricks-demo.silver_dataset.claims` c
    ON t.SRC_TransactionID = c.TransactionID
WHERE t.is_current = TRUE;

-- --------------------------------------------------------------------------------------------------

--5. Desempeño de Pagadores y Resumen de Reclamos (Gold): esta tabla monitorea el desempeño de las aseguradoras (pagadores), enfocándose en las tasas de aprobación de reclamos, los montos pagados y la eficiencia del procesamiento.

CREATE TABLE IF NOT EXISTS `avd-databricks-demo.gold_dataset.payor_performance` AS
SELECT
    c.PayorID,
    c.PayorType,
    COUNT(DISTINCT c.Claim_Key) AS TotalClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) AS ApprovedClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Denied' THEN c.Claim_Key END) AS DeniedClaims,
    COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Pending' THEN c.Claim_Key END) AS PendingClaims,
    ROUND((COUNT(DISTINCT CASE WHEN c.ClaimStatus = 'Approved' THEN c.Claim_Key END) / NULLIF(COUNT(DISTINCT c.Claim_Key), 0)) * 100, 2) AS ClaimApprovalRate,
    SUM(CAST(c.ClaimAmount AS FLOAT64)) AS TotalClaimAmount,
    SUM(CAST(c.PaidAmount AS FLOAT64)) AS TotalPaidAmount,
    SUM(CAST(c.ClaimAmount AS FLOAT64)) - SUM(CAST(c.PaidAmount AS FLOAT64)) AS OutstandingAmount
FROM `avd-databricks-demo.silver_dataset.claims` c
WHERE c.is_current = TRUE
GROUP BY c.PayorID, c.PayorType;
