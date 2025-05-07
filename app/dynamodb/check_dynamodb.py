import boto3
import argparse
import json
import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def scan_items(table_name, limit=10, region=None):
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

def main():
    parser = argparse.ArgumentParser(description='Scan DynamoDB table')
    parser.add_argument('--table', default='anomalies', help='Table name')
    parser.add_argument('--region', help='AWS region')
    parser.add_argument('--limit', type=int, default=10, help='Max items')
    
    args = parser.parse_args()
    
    region = args.region or os.environ.get('AWS_REGION', 'us-east-1')
    print(f"Using AWS region: {region}")
    
    scan_items(args.table, args.limit, region)

if __name__ == '__main__':
    main()