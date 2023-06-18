from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


def process_api_data(url, username, password):
    data = []

    while url:
        response = requests.get(url, auth=(username, password))
        response_data = response.json()
        data.extend(response_data['data'])
        next_page_url = response_data['next_page_url']
        url = next_page_url if next_page_url else None

    id_list = [item['id'] for item in data]
    column_lists = {key: [item[key] for item in data] for key in data[0].keys()}

    df = pd.DataFrame({'id': id_list, **column_lists})
    return df


def api_to_bigq():
    api_url = 'http://yourip/items'
    username = 'username'
    password = 'password'
    df = process_api_data(api_url, username, password)

    credentials = service_account.Credentials.from_service_account_file('yourkey.json')
    project_id = 'yourproject'
    client = bigquery.Client(credentials=credentials, project=project_id)

    table_id = 'yourproject.apicalisma.testtable'
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    current_date = datetime.now()
    row_count = int(len(df))
    last_id = df['id'].max()
    job_name = "Testapi"

    log_data = {"rownum": [row_count], "lastid": [last_id], "insertdate": [current_date], "jobname": [job_name]}
    log_df = pd.DataFrame(log_data)

    job_configlog = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("rownum", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("lastid", bigquery.enums.SqlTypeNames.INTEGER),
            bigquery.SchemaField("insertdate", bigquery.enums.SqlTypeNames.DATETIME),
            bigquery.SchemaField("jobname", bigquery.enums.SqlTypeNames.STRING)
        ])

    log_table_id = "yourproject.apicalisma.logtable"
    log_job = client.load_table_from_dataframe(log_df, log_table_id, job_config=job_configlog)
    log_job.result()


args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'apitobig_q',
    default_args=args,
    description='apitobig_q',
    start_date= datetime(2023, 6, 18),
    schedule= '*/15 * * * *',
    catchup=False,

) as dag:
    loadbigq = PythonOperator(
        task_id='apitobig_qry',
        python_callable=api_to_bigq,
        provide_context=True,
    )
    loadbigq
