from airflow import DAG # type: ignore
from datetime import datetime,timezone
from airflow.sensors.filesystem import FileSensor #type:ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator #type:ignore
from airflow.models import Variable #type:ignore
from airflow.models.connection import Connection #type:ignore
from dags_folder.sub_dags import subdag_parallel_dag
from airflow.operators.subdag import SubDagOperator #type:ignore
from airflow.operators.python import PythonOperator # type: ignore
from SmartFileSensor import SmartFileSensor
from slack import WebClient # type: ignore
import os
import hvac  # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor #type:ignore
from airflow.operators.bash import BashOperator #type:ignore
from airflow.utils.task_group import TaskGroup



# vault_url ="http://vault:8200"
# vault_token = 'ZyrP7NtNw0hbLUqu7N3IlTdO'
# client = hvac.Client(url=vault_url, token=vault_token)
# read_response = client.secrets.kv.read_secret_version(path="slack_token") 
# token_value=read_response["data"]["data"]["token"]

token_value=Variable.get('token')
path=Variable.get('name_path_variable',default_var='run')



defaultargs={
    'owner':'selvakumarpounraj'
    }



with DAG(
         dag_id='trigger_dag',
         start_date= datetime(2024, 5, 5),
         default_args=defaultargs,
         schedule_interval='@daily'
) as dag:
    checkFileExist=SmartFileSensor(
        task_id='check_file_exist',
         filepath=path,
         fs_conn_id='file_path_system',
         poke_interval=10,
         timeout=60*1, 
         mode='reschedule'

    )

    trigger_job=TriggerDagRunOperator(
        task_id='trigger_job',
        trigger_dag_id='student_dag',
        execution_date="{{ds}}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30
    )  



    task_group=SubDagOperator(
        task_id='task_group',
        subdag=subdag_parallel_dag('trigger_dag','task_group',defaultargs)
    )

    @dag.task #functionalDag
    def slack_connection(**context):
         
        client=WebClient(token=token_value)
        runid=context.get('task_instance').dag_id
        execution_date=context.get('execution_date')
        response=client.chat_postMessage(
           channel='airflowproject',
           text=f"runid: {runid} & Execution date: {execution_date}"
        )
    
    
    slack_notification=slack_connection()

    checkFileExist>>trigger_job>>task_group>>slack_notification

#task Group code
    # with TaskGroup(group_id="subdag") as sub_dag_processing:

    #     def printresult(ti,**kwargs):
    #         run_id=ti.xcom_pull(key='run_id',dag_id='student_dag',task_ids='return_run_id')
    #         run_id=kwargs["run_id"]
    #         print(f"job_dag runid :{run_id}")
    #         for key,value in kwargs.items():
    #             print(f"-{key} : {value}")
   

    
    #     def get_exection_date(current_execution_date):
    #         temp=current_execution_date.strftime("%Y-%m-%d")
    #         return datetime.strptime(temp,"%Y-%m-%d").replace(tzinfo=timezone.utc)
 
    #     dag_status = ExternalTaskSensor(
    #         task_id = "Sensor_triggered_dag",
    #         external_dag_id="student_dag",  
    #         external_task_id = None,
    #         execution_date_fn= get_exection_date,
    #         timeout = 120
    #     )
 
    #     print_result = PythonOperator(
    #         task_id = "print_result",
    #         python_callable = printresult,
    #         provide_context=True,
    #     )
 
    #     Remove_Run_File = BashOperator (
    #         task_id = "Remove_Run_File",
    #         bash_command="rm  /opt/airflow/plugins/run",
    #     )
 
    #     timestamp_status = BashOperator(
    #         task_id = "Run_time",
    #         bash_command='echo "Execution timestamp without dashes: {{ ts_nodash }}"',
    #     )
 
    #     dag_status >> print_result >> Remove_Run_File >> timestamp_status
    # checkFileExist>>trigger_job>>sub_dag_processing>>slack_notification