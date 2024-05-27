import argparse
import json
import csv
import joblib
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException

def load_model_from_file(model_filename):
    return joblib.load(model_filename)


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

def apply_model(model, data, datatypes):
    X = [[convert_to_original_datatypes(value, datatype) for value, datatype in zip(data.values(), datatypes)]]
    predictions = model.predict(X)
    return predictions

def msg_process(msg, model):
    key = json.loads(msg.key())
    value = msg.value()

    if key is not None and value is not None:
        try:
            key_datatypes = json.loads(key)
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON from message key: {e}")
            return

        data = json.loads(value)
        predictions = apply_model(model, data, key_datatypes)
        print(f"Predictions: {predictions}")
    else:
        print("No key or value found in the message.")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str, help='Name of the Kafka topic to consume.')
    parser.add_argument('model_filename', type=str, help='Filename of the pre-trained model (joblib file).')
    args = parser.parse_args()

    model = load_model_from_file(args.model_filename)

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}
    consumer = Consumer(conf)

    try:
        consumer.subscribe([args.topic])

        while True:
            msg = consumer.poll(1000)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}')
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f'Topic unknown, creating {args.topic} topic')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg, model)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    main()
