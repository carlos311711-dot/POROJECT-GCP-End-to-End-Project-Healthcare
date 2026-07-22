# Diagrama ER — Modelo Estrella (Capa Gold)

Modelo estrella derivado de `data/BQ/gold.sql`. La tabla de hechos central es `transactions` (silver), rodeada por las dimensiones `patients`, `providers`, `departments`, `encounters` y `claims`. Las 4 tablas gold son agregaciones construidas sobre este modelo.

## Modelo Estrella (capa Silver — fuente de Gold)

```mermaid
erDiagram
    PATIENTS ||--o{ TRANSACTIONS : "PatientID"
    PROVIDERS ||--o{ TRANSACTIONS : "ProviderID"
    DEPARTMENTS ||--o{ TRANSACTIONS : "DeptID"
    PATIENTS ||--o{ ENCOUNTERS : "PatientID"
    PROVIDERS ||--o{ ENCOUNTERS : "ProviderID"
    DEPARTMENTS ||--o{ ENCOUNTERS : "DepartmentID"
    DEPARTMENTS ||--o{ PROVIDERS : "DeptID"
    TRANSACTIONS ||--o{ CLAIMS : "SRC_TransactionID = TransactionID"

    TRANSACTIONS {
        string Transaction_Key PK
        string SRC_TransactionID
        string PatientID FK
        string ProviderID FK
        string DeptID FK
        int VisitDate
        int ServiceDate
        float Amount
        float PaidAmount
        bool is_quarantined
    }
    PATIENTS {
        string Patient_Key PK
        string FirstName
        string LastName
        string Gender
        int DOB
        string Address
        bool is_current
    }
    PROVIDERS {
        string ProviderID PK
        string FirstName
        string LastName
        string Specialization
        string DeptID FK
    }
    DEPARTMENTS {
        string Dept_Id PK
        string Name
        bool is_quarantined
    }
    ENCOUNTERS {
        string Encounter_Key PK
        string PatientID FK
        string ProviderID FK
        string DepartmentID FK
        int EncounterDate
        string EncounterType
    }
    CLAIMS {
        string Claim_Key PK
        string TransactionID FK
        string ClaimStatus
        string ClaimAmount
        string PaidAmount
        string PayorType
    }
```

## Tablas Gold (agregaciones)

```mermaid
flowchart TB
    subgraph SILVER[silver_dataset]
        T[transactions - HECHOS]
        P[patients]
        PR[providers]
        D[departments]
        E[encounters]
        C[claims]
    end
    subgraph GOLD[gold_dataset]
        G1[provider_charge_summary<br/>Monto facturado por proveedor y depto]
        G2[patient_history<br/>Historial completo del paciente]
        G3[provider_performance<br/>Encuentros, facturado, tasa aprobacion]
        G4[department_performance<br/>Volumen e ingresos por depto]
    end
    T --> G1
    PR --> G1
    D --> G1
    P --> G2
    E --> G2
    T --> G2
    C --> G2
    PR --> G3
    E --> G3
    T --> G3
    C --> G3
    D --> G4
    E --> G4
    T --> G4
```

## Tablas finales Gold (solo gold_dataset)

Las tablas gold no tienen llaves foráneas entre sí: son agregaciones desnormalizadas e independientes. Las líneas punteadas indican relaciones conceptuales (comparten proveedor/departamento), no FKs reales.

```mermaid
erDiagram
    PROVIDER_CHARGE_SUMMARY {
        string Provider_Name
        string Dept_Name
        float Amount
    }

    PATIENT_HISTORY {
        string Patient_Key PK
        string FirstName
        string LastName
        string Gender
        int DOB
        string Address
        int EncounterDate
        string EncounterType
        string Transaction_Key
        int VisitDate
        int ServiceDate
        float BilledAmount
        float PaidAmount
        string ClaimStatus
        string ClaimAmount
        string ClaimPaidAmount
        string PayorType
    }

    PROVIDER_PERFORMANCE {
        string ProviderID PK
        string FirstName
        string LastName
        string Specialization
        int TotalEncounters
        int TotalTransactions
        float TotalBilledAmount
        float TotalPaidAmount
        int ApprovedClaims
        int TotalClaims
        float ClaimApprovalRate
    }

    DEPARTMENT_PERFORMANCE {
        string Dept_Id PK
        string DepartmentName
        int TotalEncounters
        int TotalTransactions
        float TotalBilledAmount
        float TotalPaidAmount
        float AvgPaymentPerTransaction
    }

    PROVIDER_PERFORMANCE ||..o{ PROVIDER_CHARGE_SUMMARY : "mismo proveedor (conceptual)"
    DEPARTMENT_PERFORMANCE ||..o{ PROVIDER_CHARGE_SUMMARY : "mismo departamento (conceptual)"
```

> Nota: las tablas 5 (Métricas Financieras) y 6 (Desempeño de Pagadores) están declaradas en `gold.sql` solo como comentarios, aún sin implementar.
