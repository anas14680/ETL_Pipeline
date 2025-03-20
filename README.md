# ETL_Pipeline

## Project Description

The purpose of this project is to download nppes data from their CMS website. The dataset contains detailed information of healthcare providers who are identified by their NPI - a unique ID. CMS provides weekly files to download NPI data from their website and use it for analytical purposes. In this project, we establish an pipeline that extracts this data from the source, tramsforms it and loads it into the required datawarehouse or datalake. We use the following cloud resources and tools to set up this pipeline. 

- AWS S3 bucket
- AWS Lambda
- Apache Airflow
- Databricks
  
## Process

The file is downloaded into an s3 bucket. We use AWS lambda function to unzip the file inside the s3 bucket. We use AWS lambda as these files are large and require alot of compute power. Given that AWS is serverless compute instance, it automatically manages the server and compute power required. A databricks notebook is established to read these datasets from S3, perform relevant transformation to create a concise table containing all the information about healthcare providers such as their specialty, address and their names. This table is then loaded into databricks unity catalog for analytical use cases. 


## Set up 

To begin the setup on your machine, we need to install relevant packages, listed in the `requirements.txt` file on our local machine. Please the following bash script. 

```pip install requirements.txt```

Once the packages have been installed, the AWS and Databricks access tokens need to be created and replaced in the `config.yaml` file.
