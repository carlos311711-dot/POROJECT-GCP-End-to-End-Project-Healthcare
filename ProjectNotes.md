# Tecnología - Stack de Ingeniería de Datos en GCP

Dominio - Gestión del Ciclo de Ingresos en Salud (RCM - Revenue Cycle Management)

RCM es el proceso que usan los hospitales para gestionar los aspectos financieros, desde que el paciente agenda una cita hasta que el proveedor recibe el pago.

Aquí un resumen simplificado:

1. Comienza con la visita del paciente:
	- se recopilan los datos del paciente y la información del seguro
	- esto asegura que el proveedor sepa quién pagará por los servicios
	- el seguro, el paciente, o ambos

2. Se prestan los servicios
	- chequeos diarios
	- tratamientos
	- cirugías
	- seguro

3. Ocurre la facturación:
	- el hospital genera una factura

4. Se revisan los reclamos (claims):
	- la aseguradora revisa la factura
	- puede aceptarla, pagarla completa, parcial, o rechazarla

5. Pagos y seguimiento
	- si la aseguradora hace un pago parcial
	- entonces el paciente paga una parte o el total restante
	- y los proveedores hacen seguimiento del pago

6. Seguimiento y mejora:
	- RCM garantiza que el hospital pueda brindar atención de calidad manteniéndose financieramente saludable

Qué nos toca hacer como Ingeniería de Datos

Tendremos datos en distintas fuentes

necesitamos crear un pipeline; el resultado de este pipeline serán tablas de hechos y tablas de dimensiones, que ayudarán al equipo de reportería a generar los KPI

## Fuentes de Datos:

1. Fuente de datos EMR - Registros Médicos Electrónicos (Cloud SQL DB)
	- Pacientes
	- Proveedores
	- Departamento
	- Transacciones
	- Encuentros (Encounter)

	- tomar el escenario simple de 1 hospital con dos sedes
		Hospital a - hospital_a_db
		Hospital b - hospital_b_db

2. Fuente de datos de Reclamaciones (Claims)
	- Compañía de seguros (archivos planos)
	- carpeta en el Datalake - Landing (una vez al mes)

3. Códigos CPT (Current Procedural Terminology)
	- sistema estandarizado usado para describir procedimientos y servicios médicos, quirúrgicos y diagnósticos realizados por profesionales de salud

3. Datos NPI - National Provider Identifier (API pública)

4. Datos ICD - los códigos ICD son un sistema estandarizado usado por los proveedores de salud para mapear el código y la descripción de un diagnóstico (API)
