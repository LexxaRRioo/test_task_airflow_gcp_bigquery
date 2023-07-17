import json

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime

def on_success_callback(context):
    ''' when a task in the DAG succeeds '''
    print(f"Task {context['task_instance_key_str']} succeeded!")

def sla_miss_callback(context):
    ''' when a task in the DAG succeeds '''
    print(f"Task {context['task_instance_key_str']} missed its SLA!")

def on_retry_callback(context):
    ''' when a task in the DAG succeeds '''
    print(f"Task {context['task_instance_key_str']} retrying...")

def on_failure_callback(context):
    ''' when a task in the DAG succeeds '''
    print(f"Task {context['task_instance_key_str']} failed!")

with open('config.json', 'r') as f:
    conf = json.load(f)
    API_ENDPOINT = conf['api_endpoint']
    API_HTTP_CONN_ID = conf['api_http_conn_id']
    DAG_SCHEDULE = conf['dag_schedule']
    DAG_SLA = conf['dag_sla']
    DAG_TIMEOUT = conf['dag_timeout']
    EMPTY_FILE_NAME = conf['empty_file_name']
    GCP_CONN_ID = conf['gcp_conn_id']
    GCS_BUCKET_LABELS = conf['gcs_bucket_labels']
    GCS_BUCKET_NAME = conf['gcs_bucket_name']
    GCS_BUCKET_STORAGE_CLASS = conf['gcs_bucket_storage_class']
    LOCATION = conf['location']
    PROJECT_ID = conf['project_id']
    SFTP_CONN_ID = conf['sftp_conn_id']
    SFTP_PATH_TO_EMPTY = conf['sftp_path_to_empty']
    SFTP_PATH_TO_TABLE = conf['sftp_path_to_table']
    TASK1_TARGET_DATASET_ID = conf['task1_target_dataset_id']
    TASK1_TARGET_TABLE_NAME = conf['task1_target_table_name']
    TASK1_TARGET_TABLE_LABELS = conf['task1_target_table_labels']
    TASK2_TARGET_DATASET_ID = conf['task2_target_dataset_id']
    TASK2_TARGET_TABLE_LABELS = conf['task2_target_table_labels']
    TASK2_TARGET_TABLE_NAME = conf['task2_target_table_name']


default_args = {
    'owner': 'RazvodovA',
    'depends_on_past': True,
    'start_date': datetime(2023, 7, 15),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'sla_miss_timeout':timedelta(minutes=DAG_SLA), 
    'timeout':timedelta(minutes=DAG_TIMEOUT), 
    'is_paused_upon_creation': True,
    'catchup': False,
    'on_success_callback': on_success_callback,
    'sla_miss_callback': sla_miss_callback,
    'on_retry_callback': on_retry_callback,
    'on_failure_callback': on_failure_callback,
    }


with DAG(
     dag_id='big_query_task_v1_0_0',
     default_args=default_args,
     schedule=DAG_SCHEDULE,
     description='This dag creates table, inserts data, moves data to history table, uploads to GCS and SFTP',
     tags=['BigQuery, GCS, SFTP']
 ):
    with TaskGroup(group_id='prepare_group') as prepare_group:
    
        create_dataset_if_not_exists_1 = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset_if_not_exists_1', 
            dataset_id=TASK1_TARGET_DATASET_ID,
            location=LOCATION,
            if_exists='ignore',
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID
        )
    
        create_dataset_if_not_exists_2 = BigQueryCreateEmptyDatasetOperator(
            task_id='create_dataset_if_not_exists_2', 
            dataset_id=TASK2_TARGET_DATASET_ID,
            location=LOCATION,
            if_exists='ignore',
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID
        )

        create_bucket = GCSCreateBucketOperator(
            task_id='create_bucket',
            bucket_name=GCS_BUCKET_NAME,
            storage_class=GCS_BUCKET_STORAGE_CLASS,
            location=LOCATION,
            labels=GCS_BUCKET_LABELS,
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID
        )        
        
        create_native_table_task1 = BigQueryCreateEmptyTableOperator(
            task_id='create_native_table_task1',
            dataset_id=TASK1_TARGET_DATASET_ID,
            table_id=TASK1_TARGET_TABLE_NAME,
            schema_fields=[
                {"name": "dt", "type": "DATE", "mode": "REQUIRED"},
                {"name": "user_id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "income", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "loading_dt", "type": "DATE", "mode": "REQUIRED", "default_value_expression": "CURRENT_DATE"}
            ],
            if_exists="ignore",
            labels=TASK1_TARGET_TABLE_LABELS,
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID
        )
        
        create_native_table_task2 = BigQueryCreateEmptyTableOperator(
            task_id='create_native_table_task2',
            dataset_id=TASK2_TARGET_DATASET_ID,
            table_id=TASK2_TARGET_TABLE_NAME,
            schema_fields=[
                {"name": "dt", "type": "DATE", "mode": "REQUIRED"},
                {"name": "user_id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "income", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "loading_dt", "type": "DATE", "mode": "REQUIRED"}
            ],
            time_partitioning = {
                    "type": "DAY",
                    "field": "loading_dt"
            },
            if_exists='ignore',
            labels=TASK2_TARGET_TABLE_LABELS,
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID
        )

        create_dataset_if_not_exists_1 >> create_dataset_if_not_exists_2 >> [ create_bucket , create_native_table_task1, create_native_table_task2 ] 



    insert_into_table_task1 = BigQueryExecuteQueryOperator(
        task_id='insert_into_table_task1',
        sql=f'''
        insert into {PROJECT_ID}.{TASK1_TARGET_DATASET_ID}.{TASK1_TARGET_TABLE_NAME} (dt, user_id, income)
        values ('2023-01-01', 1, 3000)
              ,('2023-01-01', 2, 4000)
              ,('2023-01-01', 3, 5000)
              ,('2023-02-01', 1, 3000)
              ,('2023-03-01', 1, 3000)
        ''',
        destination_dataset_table=f'{PROJECT_ID}.{TASK1_TARGET_DATASET_ID}.{TASK1_TARGET_TABLE_NAME}',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=GCP_CONN_ID,
        use_legacy_sql=False
    )

    copy_selected_data_task2 = BigQueryToBigQueryOperator(
        task_id='copy_selected_data',
        source_project_dataset_tables=f'{PROJECT_ID}.{TASK1_TARGET_DATASET_ID}.{TASK1_TARGET_TABLE_NAME}',
        destination_project_dataset_table=f'{PROJECT_ID}.{TASK2_TARGET_DATASET_ID}.{TASK2_TARGET_TABLE_NAME}'
    )

    bigquery_to_task3 = BigQueryToGCSOperator(
        task_id='bigquery_to_task3',
        source_project_dataset_table=f'{PROJECT_ID}.{TASK1_TARGET_DATASET_ID}.{TASK1_TARGET_TABLE_NAME}',
        destination_cloud_storage_uris=[f'gs://{GCS_BUCKET_NAME}/{TASK1_TARGET_TABLE_NAME}'],
        export_format='CSV',
        field_delimiter=',',
        print_header=True,
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        project_id=PROJECT_ID
    )

    create_empty_file_task4 = BashOperator(
        task_id='create_empty_file_task4',
        bash_command=f'touch empty_file.txt && gsutil cp empty_file.txt gs://{GCS_BUCKET_NAME}'
    )

    copy_from_gcs_to_sftp_task5 = GCSToSFTPOperator(
        task_id='copy_from_gcs_to_sftp_task5',
        source_bucket=GCS_BUCKET_NAME,
        source_object=EMPTY_FILE_NAME,
        destination_path=SFTP_PATH_TO_EMPTY,
        sftp_conn_id=SFTP_CONN_ID,
        gcp_conn_id=GCP_CONN_ID
    )

    wait_for_api_task6 = HttpSensor( 
        task_id='wait_for_api_task6',
        http_conn_id=API_HTTP_CONN_ID,
        endpoint=API_ENDPOINT,
        method='GET',
        response_check=lambda response: response.status_code == 200,
        mode='poke',
        timeout=300,
        poke_interval=30,
    ) 

    copy_from_gcs_to_sftp_task7 = GCSToSFTPOperator(
        task_id='copy_from_gcs_to_sftp_task7',
        source_bucket=GCS_BUCKET_NAME,
        source_object=TASK1_TARGET_TABLE_NAME,
        destination_path=SFTP_PATH_TO_TABLE,
        sftp_conn_id=SFTP_CONN_ID,
        gcp_conn_id=GCP_CONN_ID
    )
 
    prepare_group >> \
    insert_into_table_task1 >> \
    copy_selected_data_task2 >> \
    bigquery_to_task3 >> \
    create_empty_file_task4 >> \
    copy_from_gcs_to_sftp_task5 >> \
    wait_for_api_task6 >> \
    copy_from_gcs_to_sftp_task7