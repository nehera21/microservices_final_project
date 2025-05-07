"""
CLI Application
Main entry point for the S3 upload functionality
"""
import argparse
import os
import sys
import logging
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from s3_upload.uploader import S3Uploader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def setup_aws_infrastructure():
    """
    Set up the necessary AWS infrastructure:
    - S3 bucket
    
    Returns:
        bool: True if setup was successful
    """
    try:
        # Get configuration from environment variables
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'file-upload-bucket')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        
        logger.info("Setting up AWS infrastructure")
        
        # Create S3 bucket
        s3_uploader = S3Uploader(bucket_name, region)
        if not s3_uploader.create_bucket_if_not_exists():
            logger.error("Failed to create S3 bucket")
            return False
        
        logger.info("AWS infrastructure setup completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up AWS infrastructure: {e}")
        return False

def upload_file(file_path):
    """
    Upload a file to the S3 bucket
    
    Args:
        file_path (str): Path to the local file
        
    Returns:
        bool: True if upload was successful
    """
    try:
        # Get configuration from environment variables
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'file-upload-bucket')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        
        # Validate file
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            return False
        
        # Upload file
        s3_uploader = S3Uploader(bucket_name, region)
        upload_success = s3_uploader.upload_file(file_path)
        
        if not upload_success:
            logger.error(f"Failed to upload file {file_path}")
            return False
            
        logger.info(f"Successfully uploaded {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error in upload process: {e}")
        return False

def main():
    """Main entry point for the CLI application"""
    parser = argparse.ArgumentParser(description='S3 File Uploader')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Setup command
    setup_parser = subparsers.add_parser('setup', help='Set up AWS infrastructure')
    
    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload a file')
    upload_parser.add_argument('file', help='Path to the file')
    
    args = parser.parse_args()
    
    # Execute command
    if args.command == 'setup':
        if setup_aws_infrastructure():
            print("AWS infrastructure set up successfully")
        else:
            print("Failed to set up AWS infrastructure")
            return 1
    
    elif args.command == 'upload':
        if upload_file(args.file):
            print(f"File {args.file} uploaded successfully")
        else:
            print(f"Failed to upload file {args.file}")
            return 1
    
    else:
        parser.print_help()
    
    return 0

if __name__ == '__main__':
    sys.exit(main()) 