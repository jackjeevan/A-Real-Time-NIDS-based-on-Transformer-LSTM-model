import json
import csv
from kafka import KafkaProducer
import time
import joblib
import os


def pd_serializer(data):
    return str(data).encode('utf-8')



producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=pd_serializer)


file_path = '../FlowGenerator/flow.csv'

if os.path.exists(file_path):
    os.remove(file_path)
with open(file_path, 'a+'):
    pass

csv_reader = csv.reader(open(file_path))
initial_size = 0
while True:
    current_size = os.path.getsize(file_path)
    if current_size > initial_size:
        break
    print('waiting for csv file to get filled')
    time.sleep(0.5)

next(csv_reader)
while True:
    try:
        ids_data = next(csv_reader)
        if ids_data is None:
            continue
        else:
            print(ids_data)
            producer.send('data',ids_data)
            producer.flush()
            time.sleep(0.001)
    except Exception as e:
        pass
