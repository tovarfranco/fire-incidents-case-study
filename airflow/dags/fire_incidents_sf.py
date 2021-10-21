# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from datetime import datetime, timedelta

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

import json
import boto3


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

conn = BaseHook.get_connection('aws_default')
info = json.loads(conn.get_extra())

raw_refined = "fire-incidents-raw-refined-dev"
refined_stg = "fire-incidents-refined-stg-dev"
stg_dwh = "fire-incidents-stg-dwh-dev"
glue_iam_role = "AWSGlueServiceRole"

stateMachineArn = "arn:aws:states:us-west-2:104674085951:stateMachine:fire-incidents-orchestrator-dev"
region_name = "us-west-2"


def print_hello():
    return 'Process Finished!'

def aws_step_function():
    client = boto3.client('stepfunctions',region_name=info["region_name"],aws_access_key_id=conn.login, aws_secret_access_key=conn.password,aws_session_token=info["aws_session_token"])
    response = client.start_execution(
        stateMachineArn=stateMachineArn
        # name='GlueETLOrchestrator'
    )
    print("The response is:")
    print(response)
    print("The type of response is:")
    print(type(response))
    initial_status = client.describe_execution(executionArn=response["executionArn"]).get('status')
    print("The status of response is:")
    print(initial_status)
    
    while client.describe_execution(executionArn=response["executionArn"]).get('status') == 'RUNNING':
        continue
    
    final_status = client.describe_execution(executionArn=response["executionArn"]).get('status')

    print("final_status is: ")
    print(final_status)
    if final_status == 'SUCCEEDED':
        print("The jobs finish ok")
    if final_status == 'FAILED':    
        raise ValueError('File not parsed completely/correctly')


def check():
    print("The Connection is:")
    print(info)
    print(info["region_name"])
    print(info["aws_session_token"])


dag = DAG(
    'Fire_Incidents',
    default_args=default_args,
    description='Orchestrator of pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
)

hello_operator = PythonOperator(
                    task_id='Process_finished', 
                    python_callable=print_hello, 
                    dag=dag)

check_credentials = PythonOperator(
                    task_id='Check_credentials', 
                    python_callable=check, 
                    dag=dag)

# rawRefined = AwsGlueJobOperator(
#         task_id = "RawtoRefined",
#         aws_conn_id="aws_default",
#         job_name = raw_refined,
#         job_desc = f"triggering glue job {raw_refined}",
#         region_name = region_name,
#         iam_role_name = glue_iam_role,
#         num_of_dpus = 1,
#         dag = dag
#         )

# refinedStg = AwsGlueJobOperator(
#         task_id = "RefinedtoStg",
#         aws_conn_id="aws_default",
#         job_name = refined_stg,
#         job_desc = f"triggering glue job {refined_stg}",
#         region_name = region_name,
#         iam_role_name = glue_iam_role,
#         num_of_dpus = 1,
#         dag = dag
#         )

# stgDwh = AwsGlueJobOperator(
#         task_id = "StgtoDwh",
#         aws_conn_id="aws_default",
#         job_name = stg_dwh,
#         job_desc = f"triggering glue job {stg_dwh}",
#         region_name = region_name,
#         iam_role_name = glue_iam_role,
#         num_of_dpus = 1,
#         dag = dag
#         )

stepFunctionStep = PythonOperator(
                    task_id='Step_Function', 
                    python_callable=aws_step_function, 
                    dag=dag)

# check_credentials >> [glue_job_step,stepFunctionStep] >> hello_operator

check_credentials >> stepFunctionStep >> hello_operator
