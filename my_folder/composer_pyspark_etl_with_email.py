from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# Configuration
PROJECT_ID = "etl-project-463010"
REGION = "us-central1"
CLUSTER_NAME = "cluster-sample-12321"
ZONE = "us-central1-a"
BUCKET_NAME = "sample_etl_bucket1234"
GCS_SCRIPT_PATH = f"gs://{BUCKET_NAME}/scripts/employee_etl.py"
LOCAL_SCRIPT_PATH = "D:\My_Practice\employee_etl.py"  # Local path in Composer
TO_EMAIL = "shivapatel726@gmail.com"

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
    "software_config": {"image_version": "2.1-debian11"},
    "gce_cluster_config": {"zone_uri": ZONE},
}

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_SCRIPT_PATH},
}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="composer_pyspark_etl_with_email",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dataproc", "pyspark", "email"],
) as dag:


    # Step 2: Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # Step 3: Submit PySpark job
    run_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=SPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Step 4: Delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if job fails
    )

    # Step 5: Send success email
    send_success_email = EmailOperator(
        task_id="send_email",
        to=TO_EMAIL,
        subject="✅ PySpark ETL Job Completed",
        html_content="""
        <h3>ETL Job Completed</h3>
        <p>Your Dataproc PySpark job has finished successfully.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Define task flow
    create_cluster >> run_pyspark_job >> delete_cluster >> send_success_email
