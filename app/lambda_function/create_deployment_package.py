"""
Script to create a deployment package for the Lambda function
"""
import os
import shutil
import subprocess
import zipfile

def create_deployment_package():
    # Create a temporary directory for the package
    if os.path.exists('package'):
        shutil.rmtree('package')
    os.makedirs('package')
    
    # Install dependencies
    subprocess.check_call([
        'pip', 'install', 
        '-r', 'requirements.txt', 
        '--target', 'package'
    ])
    
    # Copy the lambda function to the package
    shutil.copy('lambda_function.py', 'package/')
    
    # Create a ZIP file
    with zipfile.ZipFile('lambda_function.zip', 'w') as zipf:
        for root, dirs, files in os.walk('package'):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(
                    file_path,
                    arcname=os.path.relpath(file_path, 'package')
                )
    
    # Clean up
    shutil.rmtree('package')
    
    print(f"Created deployment package: {os.path.abspath('lambda_function.zip')}")
    
if __name__ == '__main__':
    create_deployment_package() 