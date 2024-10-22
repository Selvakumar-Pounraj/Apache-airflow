from airflow import DAG # type: ignore
from airflow.operators.bash import BashOperator #type:ignore
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.python import BranchPythonOperator # type: ignore
from airflow.utils.trigger_rule import TriggerRule # type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from custom_operators import PostgresTableCountOperator 
import uuid


#variable declarations 

config = {
    'student_dag': {'schedule_interval': None, "start_date": datetime(2024, 8, 8),"table_name":"student"},  
    'teacher_dag': {'schedule_interval': None, "start_date": datetime(2024, 8, 8),"table_name":"seacher"},  
    'school_dag': {'schedule_interval': None, "start_date": datetime(2024, 8, 8),"table_name":"school"}
    }

defaultargs={
    'owner':'selvakumarpounraj',
    'queue':'jobs_queue'
    }


conn_id='postgres_localhost'
table_query="SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='{}');"



def print_daginformation(dag_name,table_name):
    print (dag_name +" start processing tables in database: "+table_name)
    
def check_table_exist(tablename,query):
        postgres_hook=PostgresHook(postgres_conn_id=conn_id)
        final_query=query.format(tablename)
        results=postgres_hook.get_first(sql=final_query)
        a=bool(results[0])
        if a==True :
            return 'insert_new_row'
        else:
            return 'create_table'
        


def return_runid(ti,**context):
    val= context.get('task_instance').run_id
    ti.xcom_push(key='run_id',value=val)
     
    


    
def create_dag(dagid,interval,startdate,table_name):
    
    with DAG(
         dag_id=dagid,
         start_date=startdate,
         default_args=defaultargs,
         schedule_interval=interval
    ) as dynamicdag:
        printDagInfo=PythonOperator(
            task_id='dag_information',
            python_callable=print_daginformation,
            op_args = [dagid,table_name]
        )

       
        insert_new_row=PostgresOperator(
             task_id='insert_new_row',
             postgres_conn_id='postgres_localhost',
             sql=f"Insert into {table_name} values (%s,%s,%s)",
             trigger_rule = TriggerRule.NONE_FAILED,
             parameters= (uuid.uuid4().int % 123456789,"{{ti.xcom_pull(task_ids='get_current_user') }}",datetime.now())
        )
         

        query_the_table=PostgresTableCountOperator(
             task_id='query_the_table',
             table_name=table_name,
             postgres_conn_id=conn_id

        )
    
        create_table=PostgresOperator(
            task_id='create_table',
            postgres_conn_id='postgres_localhost',
            sql= f"""
            create table {table_name}(
                custom_id integer NOT NULL, 
                user_name VARCHAR (50) NOT NULL,
                timestamp TIMESTAMP NOT NULL
            );
        """
    )

        

        table_exist_or_not=BranchPythonOperator(
            task_id='table_exist_or_not',
            python_callable=check_table_exist,
            op_args=[table_name,table_query]
        )

        get_current_user = BashOperator(
        task_id='get_current_user',
        bash_command='echo "$(whoami)"'
        )

        return_run_id=PythonOperator(
             task_id='return_run_id',
             python_callable=return_runid
        )

        
    printDagInfo>>get_current_user>>table_exist_or_not
    table_exist_or_not>>create_table>>insert_new_row>>query_the_table>>return_run_id
    table_exist_or_not>>insert_new_row>>query_the_table>>return_run_id
    

for dag_name,dag_value in config.items():
    dagid=f"{dag_name}"
    interval=dag_value['schedule_interval']
    startdate=dag_value['start_date']
    tablename=dag_value['table_name']
    globals()[dagid]=create_dag(dagid,interval,startdate,tablename)

   


