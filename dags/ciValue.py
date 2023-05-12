from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime

def _choose_environment(**kwargs):
    environment_type = kwargs['dag_run'].conf['environment_type']
    if environment_type == "development":
        return ["file_creation_development"]
    elif environment_type == "production":
        return ["file_creation_production"]
    else:
        raise ValueError(f"Unknown environment type: {environment_type}")

def _create_file(environment_type):
    filename = f"civalue_{environment_type}_{datetime.now()}.txt"
    with open(filename, 'w') as f:
        f.write(f"hello civalue from {environment_type} branch")
    return filename

def _print_to_console(filename):
    f = open(filename, 'r')
    content = f.read()
    print(content)

with DAG(
    dag_id='civalue_dag',
    start_date=datetime.now(),
    schedule=None,
    tags=['civalue'],
    default_args={} 
    ) as dag:
    
    environment_branch = BranchPythonOperator(
        task_id="environment_branch",
        python_callable=_choose_environment
    )


    file_creation_development = PythonOperator(
        task_id="file_creation_development",
        python_callable=_create_file,
        op_args={"environment_type": "development"}
    )

    file_creation_production = PythonOperator(
        task_id="file_creation_production",
        python_callable=_create_file,
        op_args={"environment_type": "production"}
    )

    print_to_console = PythonOperator(
        task_id="print_to_console",
        python_callable=_print_to_console,
        op_args={"filename": "{{ task_instance.xcom_pull(task_ids=['file_creation_production','file_creation_development']) }}"},
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
    
    
    environment_branch >> file_creation_development >> print_to_console
    environment_branch >> file_creation_production >> print_to_console