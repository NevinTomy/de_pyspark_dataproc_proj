from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule

# GCP and Dataproc Configuration
PROJECT_ID = Variable.get("PROJECT_ID")
REGION = "us-central1"
CLUSTER_NAME = "sales-etl-pyspark-cluster"
BUCKET_NAME = Variable.get("BUCKET_NAME")
MAIN_PYSPARK_FILE = "file:///tmp/sales_data_etl/main.py"
INIT_SCRIPT_PATH = f"gs://{BUCKET_NAME}/scripts/sales_etl_init_script.sh"

# Define cluster configuration
CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "zone_uri": "us-central1-a",
        "subnetwork_uri": "default",
        "internal_ip_only": False,
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 30},
    },
    "worker_config": {
        "num_instances": 0,
    },
    "initialization_actions": [
        {
            "executable_file": INIT_SCRIPT_PATH
        }
    ],
    "software_config": {
        "properties": {
        "dataproc:pip.packages": "mysql-connector-python==9.2.0,google-cloud-secret-manager==2.23.1",
        },
    },
}

# Define PySpark job configuration
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": MAIN_PYSPARK_FILE,
    },
}

# Define default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "sales_data_etl_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task 1: Create Dataproc cluster with Python package installation
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    # Task 2: Submit PySpark job
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=PYSPARK_JOB,
    )

    # Task 3: Delete Dataproc cluster (even if job fails)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        #trigger_rule=TriggerRule.ALL_DONE,  # Ensures cluster is deleted even if job fails
    )

    # Define DAG task dependencies
    create_cluster >> submit_pyspark_job >> delete_cluster
