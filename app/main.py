import argparse
import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from s3_upload.uploader import S3Uploader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def upload_file(file_path):
    """
    Upload a file to the S3 bucket
    
    Args:
        file_path (str): Path to the local file
        
    Returns:
        bool: True if upload was successful
    """
    try:
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'file-upload-bucket')
        region = os.environ.get('AWS_REGION', 'us-east-1')
        
        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            return False
        
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

    parser.add_argument('file', help='Path to the file')
    
    args = parser.parse_args()
    
    if upload_file(args.file):
        print(f"File {args.file} uploaded successfully")
    else:
        print(f"Failed to upload file {args.file}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main()) 