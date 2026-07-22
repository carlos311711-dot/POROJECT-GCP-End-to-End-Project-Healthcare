import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# Definir los argumentos por defecto
ARGS = {
    "owner": "SHAIK SAIDHUL",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Definir el DAG padre
with DAG(
    dag_id="parent_dag",
    schedule_interval="0 5 * * *",
    description="DAG padre que dispara los DAGs de PySpark y BigQuery",
    default_args=ARGS,
    tags=["parent", "orchestration", "etl"]
) as dag:

    # Tarea para disparar el DAG de PySpark
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
    )

    # Tarea para disparar el DAG de BigQuery
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
    )

# Definir las dependencias
trigger_pyspark_dag >> trigger_bigquery_dag
