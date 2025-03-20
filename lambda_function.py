import boto3
import zipfile
import requests
from io import BytesIO

def lambda_handler(event, context):
    # Parameters from the event (you can pass these when invoking the Lambda)
    url = event['url']  # URL of the file to download
    bucket_name = event['bucket_name']  # S3 bucket name to upload to
    s3_prefix = event.get('s3_prefix')  # Optional prefix for S3 path


    s3_client = boto3.client('s3')


    # Step 1: Download the file to /tmp and upload it to S3
    response = requests.get(url)
    if response.status_code == 200:
        print(f"Successfully downloaded file from {url}")
        


        # Upload the zipped file to S3

    s3_client.put_object(Bucket=bucket_name, Key=f"{s3_prefix}/zipped_data.zip", Body=BytesIO(response.content))
    print(f"Uploaded zipped file to s3://{bucket_name}/{s3_prefix}")
    

    s3_object = s3_client.get_object(Bucket=bucket_name, Key=f"{s3_prefix}/zipped_data.zip")
    print('file loaded')
    # Wrap the S3 object content in a BytesIO stream
    with zipfile.ZipFile(BytesIO(s3_object['Body'].read()), 'r') as zip_ref:
        # Iterate through each file in the ZIP archive
        for file_name in zip_ref.namelist():
            # Read each file as bytes
            file_data = zip_ref.read(file_name)

            # Define the S3 key for the extracted file (use the desired path and file name)
            s3_upload_key = f"{file_name}"

            # Upload the file to S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key=f"{s3_prefix}/{s3_upload_key}",
                Body=file_data
            )
            print(f"Uploaded {file_name} to s3://{bucket_name}/{s3_prefix}/{s3_upload_key}")


