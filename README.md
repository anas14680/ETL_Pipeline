# ETL_Pipeline

## Project Description

The purpose of this project is to download nppes data from their CMS website. The dataset contains detailed information of healthcare providers who are identified by their NPI - a unique ID. CMS provides weekly files to download NPI data from their website and use it for analytical purposes. In this project, we establish an pipeline that extracts this data from the source, tramsforms it and loads it into the required datawarehouse or datalake. We use the following cloud resources and tools to set up this pipeline. 

- AWS S3 bucket
- AWS Lambda
- Apache Airflow
- Databricks
  
## Process

The file is downloaded into an s3 bucket. We use AWS lambda function to unzip the file inside the s3 bucket. We use AWS lambda as these files are large and require alot of compute power. Given that AWS is serverless compute instance, it automatically manages the server and compute power required. A databricks notebook is established to read these datasets from S3, perform relevant transformation to create a concise table containing all the information about healthcare providers such as their specialty, address and their names. This table is then loaded into databricks unity catalog for analytical use cases. We create an Airflow DAG `ETL_NPI_DAG.py`, which performs serveral steps in a particular order so the data pipeline can be executed. The DAG has the following tasks in it run in the order mentioned below.

- establish_airflow_connections: uses the file `establish_airflow_connections.py`. Sets up connection of AWS and databricks with airflow.
- create_aws_dependencies: uses the file `create_dependencies.py`. Creates a folder of relevant python packages that we will need while running our lambda function.
- upload_lambda: uses the file `upload_lambda.py`. Takes the lambda function script  `lambda_function.py` (this function actually extracts the data into S3), and uploads it to AWS lambda with set configurations. 
- upload_databricks_notebook: take the jupyter notebook `transform_nppes_data.ipynb` (this notebook performs all the data transformation processes), and uploads it to databricks.
- invoke_lambda_function: runs the created lamnda function
- transform_in_databricks: runs the databricks notebook
  


## Set up 

To begin the setup on your machine, we need to install relevant packages, listed in the `requirements.txt` file on our local machine. Please the following bash script. 

```pip install requirements.txt```

Once the packages have been installed, the AWS and Databricks access tokens need to be created and replaced in the `config.yaml` file. Finally the following commands should be run in the terminal to trigger the DAG pipeline. 

This command will schedule the DAG from local machine to the airflow server. 

```airflow scheduler```

This command will be make the airflow server active. Run this in a seperate terminal window to make sure that the airflow server remains active, while running the rag.

```airflow webserver```

Lastly, replace the following variables in the `dag_trigger.sh` before running it. This is the final step and triggers the DAG pipeline.

- load_type: can be set to incremental or full_load (depends on whether we are just adding new data to our data warehouse or refreshing it fully.
- data_url: set to the link CMS website provides for the data that needs to be loaded
- time_period: as CMS provides weekly and monthly files, this variable helps us rename and organize the loaded files into S3 based on the data start and end date. The dates in this variable follow the format `yyyymmdd`.
