"""
Utility script to check DynamoDB table access and structure
"""
import boto3
import argparse
import json
import os
from decimal import Decimal
import uuid
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Helper class to convert Decimal to float for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def list_tables(region=None):
    """List all DynamoDB tables in the account"""
    region = region or os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.client('dynamodb', region_name=region)
    response = dynamodb.list_tables()
    
    print("\nAvailable DynamoDB tables:")
    print(f"Region: {region}")
    for table in response.get('TableNames', []):
        print(f"- {table}")
    print()

def check_table_exists(table_name, region=None):
    """Check if a specific table exists"""
    region = region or os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    try:
        response = dynamodb.describe_table(TableName=table_name)
        print(f"\nTable '{table_name}' exists in region {region}!")
        
        # Show key schema
        key_schema = response['Table']['KeySchema']
        print("\nKey Schema:")
        for key in key_schema:
            print(f"- {key['AttributeName']} ({key['KeyType']})")
        
        return True
    except dynamodb.exceptions.ResourceNotFoundException:
        print(f"\nTable '{table_name}' does not exist in region {region}!")
        return False

def scan_items(table_name, limit=10, region=None):
    """Scan and display items from the table"""
    region = region or os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    try:
        response = table.scan(Limit=limit)
        items = response.get('Items', [])
        
        if not items:
            print(f"\nNo items found in table '{table_name}' in region {region}")
        else:
            print(f"\nFound {len(items)} items in table '{table_name}':")
            for item in items:
                print(json.dumps(item, indent=2, cls=DecimalEncoder))
                print("-" * 40)
        
        return items
    except Exception as e:
        print(f"\nError scanning table '{table_name}': {str(e)}")
        return []

def add_test_item(table_name, region=None):
    """Add a test anomaly item to the table"""
    region = region or os.environ.get('AWS_REGION', 'us-east-1')
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    test_item = {
        'id': str(uuid.uuid4()),
        'file_name': 'test_file.csv',
        'row_number': 1,
        'column': 'temperature',
        'type': 'test_anomaly',
        'value': Decimal('999.9'),
        'description': 'This is a test anomaly item',
        'row_data': 'test,data,999.9,test',
        'detected_at': datetime.now().isoformat()
    }
    
    try:
        table.put_item(Item=test_item)
        print(f"\nSuccessfully added test item to table '{table_name}' in region {region}")
        print(json.dumps(test_item, indent=2, cls=DecimalEncoder))
        return True
    except Exception as e:
        print(f"\nError adding test item to table '{table_name}': {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Check DynamoDB table access')
    parser.add_argument('--table', default='anomalies', help='Name of the DynamoDB table to check')
    parser.add_argument('--region', help='AWS region to use')
    parser.add_argument('--list', action='store_true', help='List available tables')
    parser.add_argument('--scan', action='store_true', help='Scan items from the table')
    parser.add_argument('--add-test', action='store_true', help='Add a test item to the table')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of items to scan')
    
    args = parser.parse_args()
    
    # Use region from command line or environment
    region = args.region or os.environ.get('AWS_REGION', 'us-east-1')
    print(f"Using AWS region: {region}")
    
    # List tables if requested
    if args.list:
        list_tables(region)
    
    # Check if the specified table exists
    table_exists = check_table_exists(args.table, region)
    
    if table_exists:
        # Scan items if requested
        if args.scan:
            scan_items(args.table, args.limit, region)
        
        # Add test item if requested
        if args.add_test:
            add_test_item(args.table, region)

if __name__ == '__main__':
    main() 