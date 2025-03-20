def upload_lambda():
        
    import boto3
    from botocore.exceptions import ClientError  # Import ClientError


    # Initialize the Lambda client
    lambda_client = boto3.client('lambda')


    # Define your variables
    layer_name = 'requestlayer'
    zip_file = 'python.zip'
    lambda_func = 'lambda_function.zip'
    lambda_function_name = "extractnppesdataunzipit"  # Replace with your desired Lambda function name
    role_arn = "arn:aws:iam::221082173339:role/Lambda-access-s3"  # Replace with the ARN of your IAM role



    # List all layers and check if the layer exists
    exist_layers = lambda_client.list_layers()['Layers']
    exist_layers = [i['LayerName'] for i in exist_layers]

    if layer_name in exist_layers:
        pass
    else:
        with open(zip_file, 'rb') as layer_zip:
            response = lambda_client.publish_layer_version(
                LayerName=layer_name,
                Description='Request Dependency',
                Content={
                    'ZipFile': layer_zip.read()
                },
                CompatibleRuntimes=['python3.13'],  # Specify the compatible runtime
                CompatibleArchitectures=['x86_64']  # Specify the compatible architecture

            )



    ## retreive the existing lambda layer arn
    exist_layers = lambda_client.list_layers()['Layers']
    layer_arn = [i['LatestMatchingVersion'] for i in exist_layers if i['LayerName'] == layer_name][0]['LayerVersionArn']



    ## check if lambda function is there

    try:
        # Try to delete the function if it already exists
        lambda_client.delete_function(FunctionName=lambda_function_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            pass



    # Read the zipped file
    with open(lambda_func, 'rb') as zip_file:
        zip_file_content = zip_file.read()

    # Create the new Lambda function
    response = lambda_client.create_function(
        FunctionName=lambda_function_name,
        Runtime='python3.13',  # Replace with the runtime you want to use (e.g., python3.8)
        Role=role_arn,
        Architectures=['x86_64'],
        Handler='lambda_function.lambda_handler',  # Replace with the handler function (e.g., 'my_module.my_handler')
        Code={
            'ZipFile': zip_file_content
        },
        Layers = [layer_arn],
        Timeout=15,  # Set timeout in seconds
        MemorySize=500,  # Set memory size in MB
        EphemeralStorage={
            'Size': 512  # Size in MB (e.g., 1024 MB = 1 GB)
        }
    )


