# -*- coding: utf-8 -*-
import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from validaciones import  validacton_module
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator,PythonOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

default_args = {    
    'retries': 1,
    'owner': 'YohanSebastianProteccion',
    'start_date': days_ago(2),
    'retry_delay': datetime.timedelta(minutes=1)
}

dag = DAG(
    'dag-validaciones_factSolicitudes',
    default_args=default_args,
    description='''nothing to add''',
    schedule_interval= '@once',
    dagrun_timeout=datetime.timedelta(minutes=60))

branch_op = BranchPythonOperator(
    dag = dag,
    task_id='branching_task',
    python_callable=validacton_module.branch_func,
    provide_context=True,
    templates_dict={"query": "SELECT * FROM `afiliados-pensionados-prote.Datamart.factSolicitudes`"},)

new_record_op = DataFlowPythonOperator(
task_id = 'new_records_task',
py_file = '/home/airflow/gcs/data/validaciones_factSolicitudes.py',
gcp_conn_id='google_cloud_default',
dataflow_default_options={
    "project": 'afiliados-pensionados-prote',
    "temp_location": 'gs://bkt_prueba/temp',    
},
dag=dag
)

new_table_op = DataFlowPythonOperator(
task_id = 'new_table_task',
py_file = '/home/airflow/gcs/data/validaciones_factSolicitudes.py',
gcp_conn_id='google_cloud_default',
dataflow_default_options={
    "project": 'afiliados-pensionados-prote',
    "temp_location": 'gs://bkt_prueba/temp',    
},
dag=dag
)


branch_op >> [new_record_op, new_table_op]