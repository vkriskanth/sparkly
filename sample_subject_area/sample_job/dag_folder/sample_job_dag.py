from __future__ import annotations

import pendulum # Recommended for Airflow DAGs for timezone handling

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # Alternative

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow_admin', # Or your team name
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='sample_pyspark_job_dag',
    default_args=default_args,
    description='A sample DAG to run the sample_job PySpark application.',
    schedule=None, # Or '0 * * * *' for hourly, '@daily', etc.
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Adjust start date
    catchup=False, # Set to True if you want to backfill
    tags=['sample_pyspark', 'data_engineering'],
) as dag:

    # Define the command to run the PySpark job using the run_spark_job.py script
    # This assumes the DAG is run in an environment where:
    # 1. The repository code (including scripts/run_spark_job.py) is accessible.
    # 2. `python` and `spark-submit` (if not using DockerOperator/KubernetesPodOperator) are in PATH.
    # 3. The paths in the command are relative to the project root if the script is run from there.
    #    Airflow's execution context might vary, so absolute paths or configured base paths are safer.
    
    # For simplicity, let's assume REPO_HOME is an environment variable or Airflow variable
    # that points to the root of this data engineering repository.
    # In a real setup, this would be configured in your Airflow environment.
    repo_home_placeholder = "/opt/airflow/dags/repo" # Placeholder: Adjust to your repo's location in Airflow worker

    submit_sample_job = BashOperator(
        task_id='submit_sample_spark_job',
        bash_command=f"""
            echo "Running sample_job PySpark application..."
            # Ensure python and run_spark_job.py are executable and in PATH or use absolute paths
            # The --run flag is important to actually execute it.
            python {repo_home_placeholder}/scripts/run_spark_job.py \
                sample_subject_area/sample_job/app.py \
                local_dev \
                --job_config {repo_home_placeholder}/config/sample_job_config.yaml \
                --run
        """,
        # Optional: Set current working directory if your script relies on it
        # cwd=f'{repo_home_placeholder}', 
        doc_md="""
        ### Submit Sample Spark Job
        This task uses the `run_spark_job.py` script to submit the `sample_job/app.py`
        to a Spark cluster (or run locally based on the `local_dev` environment).
        
        - **Job Script:** `sample_subject_area/sample_job/app.py`
        - **Environment:** `local_dev`
        - **Job Configuration:** Uses `config/sample_job_config.yaml`
        
        The `run_spark_job.py` script handles the construction of the `spark-submit` command.
        Ensure that the `repo_home_placeholder` in the DAG definition correctly points to the
        root of the data engineering repository in the Airflow worker environment.
        """
    )

    # Alternative using SparkSubmitOperator (requires Airflow Spark Provider and connection setup)
    # This is a more Airflow-native way if you have a Spark connection configured in Airflow.
    # submit_sample_job_spark_operator = SparkSubmitOperator(
    #     task_id='submit_sample_spark_job_via_operator',
    #     application=f'{repo_home_placeholder}/sample_subject_area/sample_job/app.py',
    #     conn_id='spark_default',  # Your Airflow Spark connection ID
    #     application_args=[ # Arguments for your app.py, if any
    #         # "--input-path", "...",
    #         # "--output-path", "..."
    #     ],
    #     # Spark-submit configurations can be passed here directly:
    #     conf={
    #         "spark.master": "local[*]", # Example, override from connection if needed
    #         # "spark.eventLog.enabled": "true", # Example
    #     },
    #     # py_files, jars, files etc. can also be specified
    #     # py_files=f"{repo_home_placeholder}/shared_utils.zip,{repo_home_placeholder}/config", # Example
    #     verbose=True,
    # )

    # You can define more tasks here, e.g., data validation steps, notifications.
    # For this example, it's just one task.

# To make this DAG discoverable by Airflow, ensure:
# 1. This file is in Airflow's DAGs folder.
# 2. `apache-airflow` and any providers (`apache-airflow-providers-apache-spark`) are installed.
# 3. The `repo_home_placeholder` variable is correctly set or replaced.
