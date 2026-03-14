"""
Sentinel Fraud Detection Alert Handler
Reads Gold layer transactions from S3 and publishes high-risk alerts to SNS
"""

import json
import boto3
import os
import pandas as pd
from io import BytesIO
from datetime import datetime

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')


def lambda_handler(event, context):
    """
    Lambda handler triggered by S3 events from Gold layer.
    Processes JSON/Parquet files and sends alerts for high-risk transactions.
    """
    try:
        # Extract S3 bucket and key from event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Skip metadata and temporary files
        if key.startswith('_') or key.endswith('_SUCCESS'):
            return {
                'statusCode': 200,
                'body': json.dumps('Skipped metadata file')
            }
        
        print(f"Processing file: s3://{bucket}/{key}")
        
        # Read Parquet file from S3
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(obj['Body'].read()))
            transactions = df.to_dict('records')
        except Exception as e:
            print(f"Error reading file: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error reading file: {str(e)}')
            }
        
        # Filter for HIGH risk transactions
        high_risk_transactions = [
            t for t in transactions 
            if t.get('fraud_risk_flag') == 'HIGH'
        ]
        
        if len(high_risk_transactions) == 0:
            return {
                'statusCode': 200,
                'body': json.dumps('No high-risk transactions found')
            }
        
        # Send alert for each high-risk transaction
        alert_count = 0
        for transaction in high_risk_transactions:
            alert_message = format_alert(transaction)
            publish_alert(alert_message)
            alert_count += 1
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Sent {alert_count} fraud alerts')
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing file: {str(e)}')
        }


def format_alert(transaction):
    """Format transaction into a readable alert message"""
    card = transaction.get('card_number', 'N/A')
    # Mask card number
    if len(card) > 4:
        card = '*' * (len(card) - 4) + card[-4:]
    
    alert = f"""
🚨 HIGH-RISK TRANSACTION DETECTED 🚨

Card Number: {card}
Amount: ${transaction.get('total_spent', 0):.2f}
Transaction Count (10min window): {transaction.get('transaction_count', 0)}
Risk Level: {transaction.get('fraud_risk_flag', 'UNKNOWN')}

Location: {transaction.get('city', 'N/A')}
Terminal ID: {transaction.get('terminal_id', 'N/A')}

Timestamp: {datetime.now().isoformat()}

Average Transaction Value: ${transaction.get('avg_transaction_value', 0):.2f}

⚠️ Please investigate immediately!
"""
    return alert.strip()


def publish_alert(message):
    """Publish alert message to SNS topic"""
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='🚨 Sentinel: HIGH-RISK Fraud Transaction Alert',
            Message=message
        )
        print(f"Alert published successfully")
    except Exception as e:
        print(f"Failed to publish alert: {str(e)}")
        raise

