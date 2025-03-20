def establish_airflow_connections():
    

    import os

    # Get the directory of the current script
    dag_folder = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to the config file
    config_path = os.path.join(dag_folder, 'config.yaml')
    # Get the current file's absolute path

    import json
    import yaml
    from datetime import datetime
    from airflow.models import Connection
    from airflow.settings import Session


    # Load the YAML config file
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

    # Access Databricks credentials
    databricks_url = config["credentials"]["databricks"]["instance"]
    access_token = config["credentials"]["databricks"]["token"]

    # Access AWS credentials
    aws_access_key = config["credentials"]["aws"]["access_key"]
    aws_secret_key = config["credentials"]["aws"]["secret_key"]
    aws_region = config["credentials"]["aws"]["region"]

    session = Session()


    # Check if the connection already exists
    db_conn = session.query(Connection).filter_by(conn_id='databricks_default').first()
    aws_conn = session.query(Connection).filter_by(conn_id='aws_default').first()


    # Databricks connection
    if not db_conn:
        databricks_conn = Connection(
            conn_id="databricks_default",
            conn_type="databricks",
            host=databricks_url,
            extra=json.dumps({
                "token": access_token,
            })
        )

        session.add(databricks_conn)
        session.commit()


    else:
        pass


    # AWS connection
    if not aws_conn:
        aws_conn = Connection(
            conn_id="aws_default",
            conn_type="aws",
            extra=json.dumps({
                "aws_access_key_id": aws_access_key,
                "aws_secret_access_key": aws_secret_key,
                "region_name": aws_region
            })
        )

        session.add(aws_conn)
        session.commit()
    else:
        pass


    # Add connections to the session



    session.close()

