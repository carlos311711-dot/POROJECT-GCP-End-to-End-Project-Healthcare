-- Descripción: Crear tablas externas para el dataset bronze en BigQuery
-- por favor no olvides reemplazar la ruta del bucket
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.departments_ha` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-a/archive/departments/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.encounters_ha` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-a/archive/encounters/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.patients_ha` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-a/archive/patients/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.providers_ha` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-a/archive/providers/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.transactions_ha` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-a/archive/transactions/*.json']
);
---------------------------------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.departments_hb` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-b/archive/departments/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.encounters_hb` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-b/archive/encounters/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.patients_hb` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-b/archive/patients/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.providers_hb` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-b/archive/providers/*.json']
);
CREATE EXTERNAL TABLE IF NOT EXISTS `project-d92eee7b-8c90-4381-b63.bronze_dataset.transactions_hb` OPTIONS (
  format = 'JSON',
  uris = ['gs://healthcare-bucket-15032025/landing/hospital-b/archive/transactions/*.json']
);
---------------------------------------------------------------------------------------------------------------------------