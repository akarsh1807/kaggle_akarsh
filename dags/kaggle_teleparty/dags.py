dagsfrom datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 21),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'imdb_dag_teleparty',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

# getting through the connections
kaggle_api_token = Variable.get("kaggle_api_token")
postgres_conn_id = Variable.get("postgres_conn_id")
dataset_path = Variable.get("dataset_path")

# Downloading IMDb dataset from Kaggle
download_dataset = BashOperator(
    task_id='download_dataset',
    bash_command=f'kaggle datasets download -d ashirwadsangwan/imdb-dataset --unzip --force -p {dataset_path} --api-token {kaggle_api_token}',
    dag=dag,
)

def extract_necessary_data(**kwargs):
    import pandas as pd
    dataset_path = kwargs['dataset_path']
    df = pd.read_csv(f'{dataset_path}/imdb_dataset.csv')
    necessary_data = df[['title', 'rating', 'episodes']]
    return necessary_data

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_necessary_data,
    op_kwargs={'dataset_path': dataset_path},
    dag=dag,
)

def transform_data_to_json(**kwargs):
    import json
    necessary_data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    transformed_data = necessary_data.to_json(orient='records')
    return transformed_data

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data_to_json,
    dag=dag,
)

# Load data into PostgreSQL database
def load_data_into_postgres(**kwargs):
    import psycopg2
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
    conn = psycopg2.connect(
        host="localhost",
        database="imdb",
        user="postgres",
        password="password"
    )
    cur = conn.cursor()
    cur.executemany("INSERT INTO imdb_data (title, rating, episodes) VALUES (%s, %s, %s)", transformed_data)
    conn.commit()
    cur.close()
    conn.close()

load_data_into_postgres = PythonOperator(
    task_id='load_data_into_postgres',
    python_callable=load_data_into_postgres,
    dag=dag,
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "Workflow completed successfully!"',
    dag=dag,
)

dag.append(download_dataset)
dag.append(extract_data)
dag.append(transform_data)
dag.append(load_data_into_postgres)
dag.append(end_task)
