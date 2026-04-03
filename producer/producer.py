import pandas as pd
from kafka import KafkaProducer
from json import dumps
import time

df = pd.read_csv("data/sample_stock_data.csv")

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

for index, row in df.iterrows():
    data = row.to_dict()
    producer.send('demo_test', value=data)
    print(f"Sent: {data}")
    time.sleep(0.1)

producer.flush()