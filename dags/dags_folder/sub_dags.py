from airflow import DAG # type: ignore
from datetime import datetime,timezone
from airflow.operators.bash import BashOperator #type:ignore
from airflow.operators.python import PythonOperator #type:ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor #type:ignore

def subdag_parallel_dag(parent_dag_id,child_dag_id,defaultargs):
   
    def printresult(ti,**kwargs):
        run_id=ti.xcom_pull(key='run_id',dag_id='student_dag',task_ids='return_run_id')
        run_id=kwargs["run_id"]
        print(f"job_dag runid :{run_id}")
        for key,value in kwargs.items():
            print(f"-{key} : {value}")

    

    
    def get_exection_date(current_execution_date):
        temp=current_execution_date.strftime("%Y-%m-%d")
        return datetime.strptime(temp,"%Y-%m-%d").replace(tzinfo=timezone.utc)

    with DAG(dag_id=f'{parent_dag_id}.{child_dag_id}',default_args=defaultargs) as sub_dag:
        remove_file = BashOperator(
        task_id='remove_file',
        bash_command="rm /opt/airflow/plugins/run"
        )
        sensor=ExternalTaskSensor(
        task_id='sensor',
        external_dag_id='student_dag',
        external_task_id=None,
        execution_date_fn=get_exection_date
        )
        print_result=PythonOperator(
        task_id='print_result',
        python_callable=printresult,
        provide_context=True
        )
        timestamp_status = BashOperator(
            task_id = "Run_time",
            bash_command='echo "Execution timestamp without dashes: {{ ts_nodash }}"',
        )
       
        sensor>>print_result>>remove_file>>timestamp_status      
        
    

    return sub_dag