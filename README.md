IMDb Data Pipeline for Teleparty
================

This pipeline extracts the IMDb dataset from Kaggle, transforms it into a JSON format, and loads it into a PostgreSQL database.

**Prerequisites**
* Apache Airflow installed and running
* Kaggle API token set as an Airflow variable `kaggle_api_token`
* PostgreSQL database set up with a username, password, and database name
* PostgreSQL connection details set as Airflow variables `postgres_conn_id`, `postgres_username`, `postgres_password`, and `postgres_database`
* Dataset path set as an Airflow variable `dataset_path`

**How to Run**
1. Trigger the `imdb_dag_teleparty` DAG in Airflow
2. The pipeline will download the IMDb dataset from Kaggle, extract the necessary data, transform it into JSON format, and load it into the PostgreSQL database

**Notes**
* Make sure to replace the placeholders in the `dag.py` file with your actual PostgreSQL connection details and dataset path
* This pipeline assumes that the IMDb dataset is in a CSV format and has columns `title`, `rating`, and `episodes`
* You can modify the pipeline to handle errors and retries as per your requirements
