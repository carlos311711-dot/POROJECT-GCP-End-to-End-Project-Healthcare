from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

# 1. Definición de argumentos por defecto
default_args = {
    'owner': 'GTM-Digitales',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 22), # Fecha de creación
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# 2. Función de Python simple para registrar mensaje
def saludo_python():
    logging.info("=========================================")
    logging.info("¡Hola Mundo desde PythonOperator en Cloud Composer!")
    logging.info("=========================================")

# 3. Definición del DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='Un DAG simple de Hola Mundo para validar Cloud Composer',
    schedule_interval=None, # Solo ejecución manual para pruebas
    catchup=False,
    tags=['test', 'gcp', 'composer']
) as dag:

    # Tarea 1: BashOperator que imprime mensaje
    task_saludo_bash = BashOperator(
        task_id='saludo_bash',
        bash_command='echo "¡Hola Mundo desde BashOperator en Cloud Composer!"'
    )

    # Tarea 2: PythonOperator que llama a la función de Python
    task_saludo_python = PythonOperator(
        task_id='saludo_python',
        python_callable=saludo_python
    )

    # Dependencia entre tareas
    task_saludo_bash >> task_saludo_python
