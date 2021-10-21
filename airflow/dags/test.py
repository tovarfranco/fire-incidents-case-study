# import os
# import sys
# from logging import Logger
# from airflow import DAG
# from airflow.models import Variable
# from datetime import datetime
# from airflow.operators.python import BranchPythonOperator
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.utils.task_group import TaskGroup
# from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
# import pprint as pp

# sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
# from kavak.airflow.amazon.operators.glue import KavakAwsGlueJobOperator
# from lib.tables_group import TablesGroup

# WKF_NAME = "netsuite_wkf"
# WKF_VARS = Variable.get(WKF_NAME, deserialize_json=True)


# def validate_wkf_params():
#     # TODO implement validate
#     pp.pprint(WKF_VARS)

#     all_tables = TablesGroup.get_tables(WKF_VARS.get("s3_wkf_params"))
#     todo_tables = TablesGroup.get_tables(WKF_VARS.get("s3_wkf_params_todo"))

#     pp.pprint(all_tables)
#     pp.pprint("####")
#     pp.pprint(todo_tables)


# def task_branch(group_name: str) -> list:
#     todo_tables = TablesGroup.get_tables(WKF_VARS.get("s3_wkf_params_todo"))

#     return [f"{group_name}.{dag}" for dag in todo_tables]


# def create_tasks_group(
#     group_name: str, tooltip_section: str = "tables"
# ) -> TaskGroup:
#     all_tables = TablesGroup.get_tables(WKF_VARS.get("s3_wkf_params"))

#     with TaskGroup(group_id=group_name, tooltip=tooltip_section) as group:
#         for task_table in all_tables:
#             call_glue_job(
#                 task_table=task_table,
#                 job_name=WKF_VARS.get("job_name_ingestion"),
#                 script_args={
#                     "--wkf_params_netsuite": WKF_VARS.get("s3_wkf_params"),
#                     "--s3_target_data": WKF_VARS.get("s3_target_data"),
#                     "--s3_schema_tables": WKF_VARS.get("s3_schema_tables"),
#                     "--secret_name": WKF_VARS.get("secret_name"),
#                     "--execution_tables": str(
#                         {task_table: all_tables.get(task_table)}
#                     ),
#                     "--execution_date": "{{ execution_date }}",
#                 },
#             )
#     return group


# def call_glue_job(
#     task_table: str, job_name: str, script_args: dict
# ) -> KavakAwsGlueJobOperator:

#     # glue_operator = BashOperator(
#     #     task_id=f"{task_table}",
#     #     bash_command="sleep 3; echo '{{ execution_date }}';"
#     #     + f"echo '{WKF_VARS.get('s3_wkf_params_todo')}';",
#     # )

#     glue_operator = KavakAwsGlueJobOperator(
#         task_id=f"{task_table}",
#         job_name=job_name,
#         # job_name="python-glue-job",
#         script_args=script_args,
#         iam_role_name=WKF_VARS.get("iam_role_name"),
#         do_xcom_push_error=True,
#         # on_failure_callback=alert_slack_channel
#         # retries=1,
#         # retry_delay=timedelta(seconds=20),
#         # retry_exponential_backoff=True,
#         # max_retry_delay=timedelta(minutes=1),
#     )

#     return glue_operator


# def alert_slack_channel(context):
#     title = ":red_circle: *Task Failed.*" + " " + WKF_VARS.get("mentioned_user")
#     msg_parts = {
#         "Task": context.get("task_instance").task_id,
#         "DAG": context.get("task_instance").dag_id,
#         "Execution time": context.get("execution_date"),
#         "Error": context.get("ti").xcom_pull(key="ErrorMessage"),
#         "Log URL": context.get("task_instance").log_url,
#     }
#     msg = "\n".join(
#         [title, *[f"*{key}*: {value}" for key, value in msg_parts.items()]]
#     ).strip()

#     failed_alert = SlackWebhookOperator(
#         task_id="notify_slack_channel",
#         http_conn_id="slack_connection",
#         message=msg,
#         link_names=True,
#     )

#     return failed_alert.execute(context=context)


# def create_dag(
#     dag_id: str, schedule: str, default_args: dict, group_name: str, **kwargs
# ) -> DAG:

#     with DAG(
#         dag_id=dag_id,
#         schedule_interval=schedule,
#         default_args=default_args,
#         **kwargs,
#     ) as dag:

#         validate_params = PythonOperator(
#             task_id="validate_params",
#             python_callable=validate_wkf_params,
#             do_xcom_push=False,
#         )

#         tables_select = BranchPythonOperator(
#             task_id="tables_select",
#             python_callable=task_branch,
#             op_kwargs={"group_name": group_name},
#             do_xcom_push=False,
#         )

#         # tables_pages_cols = call_glue_job(
#         #     task_table="tables_pages_cols",
#         #     job_name=WKF_VARS.get("job_name_columns"),
#         #     script_args={
#         #         "--job_name": WKF_VARS.get("job_name_columns"),
#         #         "--s3_schema_tables": WKF_VARS.get("s3_schema_tables"),
#         #         "--s3_secrets": WKF_VARS.get("s3_secrets"),
#         #         "--secret_name": WKF_VARS.get("secret_name"),
#         #         "--tables_to_paginate": WKF_VARS.get("tables_to_paginate"),
#         #         # TODO deprecate and put in config file
#         #         "--env": "netsuite",
#         #         "--conn_type": "netsuite",
#         #         "--driver": "com.netsuite.jdbc.openaccess.OpenAccessDriver",
#         #         "--enable_secret_manager": "true",
#         #         "--max_columns_select": "200",
#         #     },
#         # )

#         group = create_tasks_group(group_name)

#         # validate_params >> tables_pages_cols >> tables_select >> group
#         validate_params >> tables_select >> group

#     return dag


# dag = create_dag(
#     dag_id=WKF_NAME,
#     schedule="@daily",
#     default_args={
#         "owner": "airflow",
#         "start_date": datetime(2021, 7, 1, 0, 0, 0),
#     },
#     group_name="netsuite_tables",
#     tags=["datalake", "data-office"],
#     catchup=False,
# )
