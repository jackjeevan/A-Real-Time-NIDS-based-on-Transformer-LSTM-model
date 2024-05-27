import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket

def infer_datatypes(csv_file, wait_time=300, max_attempts=3):
    # Infers data types for each column based on the first non-header row
    for attempt in range(max_attempts):
        try:
            with open(csv_file, 'r') as file:
                reader = csv.reader(file)
                header = next(reader)
                example_row = next(reader)

            datatypes = [type(value).__name__ for value in example_row]

            return header, datatypes
        except StopIteration:
            print(f"CSV file is empty. Waiting for {wait_time} seconds and retrying ({attempt+1}/{max_attempts}).")
            time.sleep(wait_time)

    return None

def convert_to_original_datatypes(value, original_datatype):
    # Convert the value to its original data type
    try:
        if original_datatype == 'float':
            return float(value)
        elif original_datatype == 'int':
            return int(value)
        # Add more datatype conversions as needed
        else:
            return value  # For other data types, keep it as is
    except ValueError:
        return value  # Return the original value if conversion fails

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='CSV file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    result = infer_datatypes(args.filename)

    if result is None:
        print("CSV file is empty after multiple attempts. Exiting.")
        sys.exit(1)

    header, header_datatypes = result

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    rdr = csv.reader(open(args.filename))
    next(rdr)  # Skip header

    while True:
        try:
            line = next(rdr, None)
            if line is None:
                break

            # Exclude the last field (Label) from being produced
            result = {header[i]: convert_to_original_datatypes(line[i], header_datatypes[i]) for i in range(len(header) - 1)}
            jresult = json.dumps(result)

            producer.produce(topic, key=p_key, value=jresult, callback=acked)
            producer.flush()
            time.sleep(4)

        except StopIteration:
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()