"""
Script to create a deployment package for the Lambda function on AWS
"""
import os
import shutil
import subprocess
import zipfile

def create_deployment_package():
    if os.path.exists('package'):
        shutil.rmtree('package')
    os.makedirs('package')
    
    subprocess.check_call([
        'pip', 'install', 
        '-r', 'requirements.txt', 
        '--target', 'package'
    ])
    
    shutil.copy('lambda_function.py', 'package/')
    
    with zipfile.ZipFile('lambda_function.zip', 'w') as zipf:
        for root, dirs, files in os.walk('package'):
            for file in files:
                file_path = os.path.join(root, file)
                zipf.write(
                    file_path,
                    arcname=os.path.relpath(file_path, 'package')
                )
    
    shutil.rmtree('package')
    
    print(f"Created deployment package: {os.path.abspath('lambda_function.zip')}")
    
if __name__ == '__main__':
    create_deployment_package() 