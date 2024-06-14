from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
import pendulum
import os

# Paths
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DIR_DAGS = f"{AIRFLOW_HOME}/dags"
DIR_JOBS = f"{AIRFLOW_HOME}/etl"
JDBC_DRIVER_PATH = f"{AIRFLOW_HOME}/etl/postgresql-42.7.3.jar"  # Update this path to your JDBC driver location
DIR_CONFIG = f"{AIRFLOW_HOME}/config"
default_py_files = f"{DIR_CONFIG}/config_services.py"

# DAG's default arguments
default_args = {
    "start_date": pendulum.now(),
    "schedule": "@daily",
    "retries": 0
}

dag = DAG(
    dag_id='test_flow',
    description="test flow ingest data from db",
    default_args=default_args
)


"""Create Hive tables in data warehouse"""
with open(f"{DIR_DAGS}/hql/create_hive_tbls.hql", "r") as script:
    create_hive_tbls = HiveOperator(
        task_id = "create_hive_tbls",
        hql = script.read(),
        dag=dag
    )


ingest_data = SparkSubmitOperator(
    task_id="ingest_data",
    name="Load airports dim table to data warehouse",
    application=f"{DIR_JOBS}/ingest_data.py",
    jars=JDBC_DRIVER_PATH,  # Specify the path to the JDBC driver
    conn_id='spark_default',
    py_files = default_py_files,
    dag=dag  
)

# Set task dependencies
create_hive_tbls >> ingest_data
