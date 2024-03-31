import json
import pathlib
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import raw_consommation

dag = DAG(
 dag_id="download_current_data",
 start_date=airflow.utils.dates.days_ago(14),
 schedule_interval=None,
)



load_data = PythonOperator(
 task_id="load_data_current",
 python_callable=raw_consommation.load_data,
 dag=dag,
)
clean_data = PythonOperator(
 task_id="load_data_current",
 python_callable=raw_consommation.load_data,
 dag=dag,
)


load_data >> clean_data >> save_data_to_mysql