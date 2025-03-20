def upload_db_nb():
        
    import base64
    import requests
    import yaml
    import os

    # Get the directory of the current script
    dag_folder = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the config file
    config_path = os.path.join(dag_folder, 'config.yaml')
    file_path   = os.path.join(dag_folder, 'transform_nppes_data.ipynb')



    # Load the YAML config file
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Access Databricks credentials
    databricks_url = config["credentials"]["databricks"]["instance"]
    access_token = config["credentials"]["databricks"]["token"]

    # File and destination path
    destination_path = "/Workspace/Users/mohammadanas109@gmail.com/transform_nppes_data"

    # Read the notebook file
    with open(file_path, "rb") as f:
        content = f.read()

    # Base64 encode the content
    encoded_content = base64.b64encode(content).decode("utf-8")

    # Prepare API request payload
    payload = {
        "path": destination_path,
        "format": "JUPYTER",
        "content": encoded_content,
        "overwrite": True
    }

    # Make the API request
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.post(
        f"{databricks_url}/api/2.0/workspace/import",
        headers=headers,
        json=payload
    )

