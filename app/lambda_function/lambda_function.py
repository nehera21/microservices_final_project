import json
import boto3
import csv
import io
import uuid
import logging
from datetime import datetime
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Lambda function that gets triggered when a file is uploaded to S3.
    Detects temperature anomalies in CSV data and stores them in DynamoDB.
    Anomalies are defined as temperature values below -10 or above 110.

    THIS FUNCTION IS NOT USED LOCALY. IT IS USED ON AWS LAMBDA.
    """
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    s3_client = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('anomalies')
    
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        file_content = response['Body'].read().decode('utf-8')
        logger.info(f"Successfully read file content, size: {len(file_content)} bytes")
        
        csv_reader = csv.reader(io.StringIO(file_content))
        
        headers = next(csv_reader, None)
        if not headers:
            raise ValueError("CSV file is empty or has no headers")
        
        logger.info(f"CSV headers: {headers}")
        
        temp_column_index = None
        for i, header in enumerate(headers):
            if header == "AvgTemperature":
                temp_column_index = i
                break
        
        if temp_column_index is None:
            raise ValueError("Could not find AvgTemperature column in CSV file")
        
        anomalies = []
        row_num = 1
        processed_rows = 0
        
        for row in csv_reader:
            row_num += 1
            processed_rows += 1
            
            if len(row) <= temp_column_index:
                logger.warning(f"Row {row_num} has insufficient columns: {row}")
                continue
            
            try:
                temp_value = float(row[temp_column_index])
                
                if temp_value < -10 or temp_value > 110:
                    logger.info(f"Anomaly detected at row {row_num}: temperature {temp_value}")
                    
                    row_data = {}
                    for i, header in enumerate(headers):
                        if i < len(row):
                            row_data[header] = row[i]
                    
                    anomaly = {
                        'id': str(uuid.uuid4()),
                        'file_name': key,
                        'row_number': row_num,
                        'type': 'temperature_anomaly',
                        'temperature': Decimal(str(temp_value)),
                        'description': f"Temperature value {temp_value} is outside normal range (-10 to 110)",
                        'detected_at': datetime.now().isoformat()
                    }
                    
                    region = row_data.get('Region', 'Unknown')
                    country = row_data.get('Country', 'Unknown')
                    state = row_data.get('State', 'Unknown')
                    city = row_data.get('City', 'Unknown')
                    
                    anomaly['region'] = region
                    anomaly['country'] = country
                    anomaly['state'] = state
                    anomaly['city'] = city
                    anomaly['month'] = row_data.get('Month', 'Unknown')
                    anomaly['day'] = row_data.get('Day', 'Unknown')
                    anomaly['year'] = row_data.get('Year', 'Unknown')
                    anomaly['location'] = f"{city}, {state}, {country}, {region}"
                    
                    anomalies.append(anomaly)
            except (ValueError, TypeError) as e:
                logger.warning(f"Row {row_num}, error parsing temperature: {str(e)}, value: {row[temp_column_index]}")
                pass
        
        logger.info(f"Processed {processed_rows} rows, found {len(anomalies)} anomalies")
        
        if anomalies:
            logger.info(f"Storing {len(anomalies)} anomalies in DynamoDB")
            with table.batch_writer() as batch:
                for anomaly in anomalies:
                    batch.put_item(Item=anomaly)
            logger.info("Successfully stored anomalies in DynamoDB")
        else:
            logger.info("No anomalies found, nothing to store in DynamoDB")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed file from S3',
                'bucket': bucket,
                'key': key,
                'rows_processed': processed_rows,
                'anomalies_found': len(anomalies)
            })
        }
    
    except Exception as e:
        logger.error(f"Error processing file {key} from bucket {bucket}: {str(e)}", exc_info=True)
        
        try:
            logger.info("Attempting to record error in DynamoDB")
            table.put_item(
                Item={
                    'id': str(uuid.uuid4()),
                    'file_name': key,
                    'type': 'processing_error',
                    'description': str(e),
                    'detected_at': datetime.now().isoformat()
                }
            )
            logger.info("Successfully recorded error in DynamoDB")
        except Exception as db_error:
            logger.error(f"Failed to record error in DynamoDB: {str(db_error)}", exc_info=True)
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error processing file: {str(e)}',
                'bucket': bucket,
                'key': key
            })
        } 