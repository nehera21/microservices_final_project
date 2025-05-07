"""
S3 Uploader
Handles uploading files to an S3 bucket
"""
import os
import boto3
from botocore.exceptions import ClientError
import logging
import csv
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Uploader:
    """Handles uploading files to an S3 bucket"""
    
    def __init__(self, bucket_name, region="us-east-1"):
        """
        Initialize the uploader with bucket name and region
        
        Args:
            bucket_name (str): Name of the S3 bucket
            region (str): AWS region
        """
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
    
    def create_bucket_if_not_exists(self):
        """
        Create the S3 bucket if it doesn't exist
        
        Returns:
            bool: True if creation was successful or bucket already exists
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket {self.bucket_name} already exists")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Creating bucket {self.bucket_name} in region {self.region}")
                try:
                    if self.region == "us-east-1":
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.region}
                        )
                    logger.info(f"Successfully created bucket {self.bucket_name}")
                    return True
                except ClientError as e:
                    logger.error(f"Error creating bucket: {e}")
                    return False
            else:
                logger.error(f"Error checking bucket: {e}")
                return False
    
    def upload_file(self, file_path, object_name=None):
        """
        Upload a file to the S3 bucket, filtering to only include Wisconsin data
        
        Args:
            file_path (str): Path to the local file
            object_name (str, optional): The object name in S3, defaults to file basename
            
        Returns:
            bool: True if file was uploaded successfully
        """
        if object_name is None:
            object_name = os.path.basename(file_path)

        try:
            # Read the CSV file
            with open(file_path, 'r') as file:
                csv_reader = csv.reader(file)
                header = next(csv_reader)  # Get the header row
                
                # Create a temporary file with only Wisconsin data
                temp_file_path = f"{file_path}.wisconsin.tmp"
                with open(temp_file_path, 'w', newline='') as temp_file:
                    csv_writer = csv.writer(temp_file)
                    csv_writer.writerow(header)  # Write the header
                    
                    # Filter for Wisconsin, US data
                    for row in csv_reader:
                        if len(row) >= 3 and row[1] == "US" and row[2] == "Wisconsin":
                            csv_writer.writerow(row)
            
            # Upload the filtered file
            logger.info(f"Uploading Wisconsin data from {file_path} to {self.bucket_name}/{object_name}")
            self.s3_client.upload_file(temp_file_path, self.bucket_name, object_name)
            logger.info(f"Wisconsin data uploaded successfully to {self.bucket_name}/{object_name}")
            
            # Clean up the temporary file
            os.remove(temp_file_path)
            return True
            
        except ClientError as e:
            logger.error(f"Error uploading file: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"File {file_path} not found")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing file: {e}")
            return False