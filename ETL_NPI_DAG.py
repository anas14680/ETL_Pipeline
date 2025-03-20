import json
import yaml
import requests
from datetime import datetime

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

from create_dependencies import create_dependencies
from establish_airflow_conns import establish_airflow_connections
from upload_db_notebook import upload_db_nb
from upload_lambda      import upload_lambda



### Retrieve databricks username and cluster id

with open("/Users/mohammadanas/Desktop/Nppes_proj/config.yaml", "r") as file:
    config = yaml.safe_load(file)

# Access Databricks credentials
databricks_url = config["credentials"]["databricks"]["instance"]
access_token = config["credentials"]["databricks"]["token"]

# Headers for authentication
headers = {
    "Authorization": f"Bearer {access_token}"
}

# Endpoint to list clusters
cluster_endpoint = f"{databricks_url}/api/2.0/clusters/list"
user_endpoint    = f"{databricks_url}/api/2.0/preview/scim/v2/Me"

# Send a GET request to fetch cluster details
cluster_response = requests.get(cluster_endpoint, headers=headers)
user_response = requests.get(user_endpoint, headers=headers)

# define cluster id and databricks user
cluster_id = cluster_response.json().get("clusters", [])[0]['cluster_id']
db_user    = user_response.json()['emails'][0]['value']





### Define the DAG
with DAG(
    dag_id ='npi_reference_table_etl',  
    start_date=datetime(2023, 12, 1),
    description='NPPES data processing pipeline',
    schedule_interval=None,  
    catchup=False,  
) as dag:

    ## Establish Airflow Connections
    establish_connections = PythonOperator(
        task_id='establish_airflow_connections',
        python_callable=establish_airflow_connections
    )

    # Create a dependency package 
    create_aws_dependencies = PythonOperator(
        task_id='create_dependencies_for_aws_lambda',
        python_callable=create_dependencies
    )

    # Create a lambda function in AWS
    upload_lambda = PythonOperator(
        task_id='create_lambda_function',
        python_callable=upload_lambda
    )

    # Create a lambda function in AWS
    upload_databricks_notebook = PythonOperator(
        task_id='upload_main_transformation_notebook_to_databricks',
        python_callable=upload_db_nb
    )




    # invoke lambda function to download zip file, unzip and upload to s3
    invoke_lambda_function = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda',
    function_name='extractnppesdataunzipit',
    invocation_type='RequestResponse',
    payload=json.dumps({
        "url": Variable.get("data_url"),
        "bucket_name": "nppes-data",
        "s3_prefix": Variable.get("time_period")

    }),
    aws_conn_id='aws_default',
    log_type='Tail'  # Optional: capture logs
    )



   
    # Extract NPPES Data
    transform_in_databricks = DatabricksSubmitRunOperator(
        databricks_conn_id='databricks_default',
        task_id='Run_databricks_compute', 
        existing_cluster_id=cluster_id,
        notebook_task={
            "notebook_path"  : f"/Workspace/Users/{db_user}/transform_nppes_data",
            "base_parameters": {
                "s3_loc": Variable.get("time_period"),
                "load_type": Variable.get("load_type")
            }
        }
    )



    establish_connections >> create_aws_dependencies >> upload_lambda >> upload_databricks_notebook
    upload_databricks_notebook >> invoke_lambda_function >> transform_in_databricks