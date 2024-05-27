import json
import csv
import pandas as pd
from kafka import KafkaConsumer
from kafka import KafkaProducer
import time
import joblib
from io import StringIO
import pickle
from elasticsearch import Elasticsearch
import uuid



def infer_datatypes(csv_file):
            with open(csv_file, 'r') as file:
                reader = csv.reader(file)
                header = next(reader)
                example_row = next(reader)

            datatypes = [type(value).__name__ for value in example_row]
            return datatypes


original_type = infer_datatypes('flow.csv')


def convert_to_original_datatypes(value, original_datatype):
    try:
        if original_datatype == 'float':
            return float(value)
        elif original_datatype == 'int':
            return int(value)
        else:
            return value
    except ValueError:
        return value


data_frame = pd.read_csv('flow.csv')
header_item = data_frame.columns
# print(header_item)

# print(header_item)
def str_deserializer(data):
    return data


def convert_to_original_datatypes(value, original_datatype):
    try:
        if original_datatype == 'float':
            return float(value)
        elif original_datatype == 'int':
            return int(value)
        else:
            return value
    except ValueError:
        return value

def json_serializer(data):
    return json.dumps(data).encode('utf-8')



consumer = KafkaConsumer('data',bootstrap_servers=['localhost:9092'],value_deserializer = str_deserializer,max_poll_records = 100)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=json_serializer)

def transform_pd(data,data_header):
    str_data = data
    str_data = str_data[1:len(str_data)-1]


    mylist = str_data.split(',')


    newlist = []
    for i,item in enumerate(mylist):
        item = convert_to_original_datatypes(item,original_type[i])
        newlist.append(item[2:len(item)-1])
    data_frame = pd.DataFrame(columns=data_header)
    data_frame = data_frame._append(pd.Series(newlist,index=data_frame.columns),ignore_index=True)
    # print(data_frame.columns)
    return data_frame




def get_label(label):
    labels = ['normal','Dos Attack','Portscan']
    return labels[label]


def push_to_kafka_topic(topic_name,data_temp,label):
    my_dict = {}
    my_dict['src_ip'] = data_temp.iloc[0,0]
    my_dict['src_port'] = data_temp.iloc[0,1]
    my_dict['dest_ip'] = data_temp.iloc[0,2]
    my_dict['dest_port'] = data_temp.iloc[0,3]
    my_dict['protocol'] = data_temp.iloc[0,4]
    my_dict['timestamp'] = data_temp.iloc[0,5]

    label_d = get_label(label)
    my_dict['label'] = label_d

    return my_dict


model = pickle.load(open('new_trained.pkl','rb'))

file = open('result','w+')

elk_client = Elasticsearch("http://localhost:9200")
if elk_client.indices.exists(index="rids"):
    elk_client.indices.delete(index="rids")
for message in consumer:
    data = message.value
    str_data = data.decode('UTF-8')
    data_frame = transform_pd(str_data, header_item)
    # print(data_frame)

    data_temp = data_frame

    data_frame = data_frame.drop(['Src IP', 'Src Port', 'Dst IP', 'Protocol', 'Timestamp'],axis=1)
    data_frame = data_frame.iloc[:,:-1]
    # print(data_frame)
    result = model.predict(data_frame)
    # print(result)
    # if(result[0] == 0):
    #     file.write(f"normal\n")
    # else:
    #     file.write(f"result[0]\n")
    kafka_data = push_to_kafka_topic('toadd',data_temp,result[0])
    elk_client.index(index="rids",id=uuid.uuid4(),document=kafka_data)
    # producer.send('log_data',kafka_data)
    print(f"pushed {kafka_data}")
