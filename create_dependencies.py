def create_dependencies():
    import os
    import subprocess
    import shutil
    import zipfile

    # Get the directory of the current script
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    os.chdir(dag_folder)

    directory = 'python/lib/python3.13/site-packages'
    base_directory = 'python'
    zipped_directory = 'python.zip'
    lambda_code = 'lambda_function.py'
    zipped_lambda_code = 'lambda_function.zip'

    # Create the subdirectory and folder
    os.makedirs(directory, exist_ok=True)

    # install 
    subprocess.check_call([
        "pip", "install", "requests", "--target", directory
    ])

    # Compress the directory into a request dependency package file
    shutil.make_archive(zipped_directory.replace('.zip', ''), 'zip', root_dir='.', base_dir=base_directory)

    # Create a zip file with the Python file
    shutil.make_archive(zipped_lambda_code.replace('.zip', ''), 'zip', root_dir='.', base_dir=lambda_code)



