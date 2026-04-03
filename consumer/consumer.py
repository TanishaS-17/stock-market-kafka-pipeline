from kafka import KafkaConsumer
from json import loads
import json
import boto3

consumer = KafkaConsumer(
    'demo_test',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

s3 = boto3.client('s3')
bucket_name = "your-bucket-name"  # CHANGE THIS

for i, message in enumerate(consumer):
    data = message.value
    
    file_name = f"stock_data_{i}.json"
    
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=json.dumps(data)
    )
    
    print(f"Stored in S3: {data}")