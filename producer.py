#!/usr/bin/env python

import sys
import time
import json
import os
import random
import requests as r
import datetime as dt
import glob
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

TRIMET_API = "http://www.psudataeng.com:8000/getBreadCrumbData"
KAFKA_TOPIC = "sensor-data"


class KafkaProducer:
    def __init__(self):
        # Initialize Kafka producer here
        # Parse the command line.
        parser = ArgumentParser()
        parser.add_argument('config_file', type=FileType('r'))
        args = parser.parse_args()

        # Parse the configuration.
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        config_parser = ConfigParser()
        config_parser.read_file(args.config_file)
        config = dict(config_parser['default'])

        # Create Producer instance
        self.producer = Producer(config)
        self.delivered_records = 0

    def delivery_callback(self,err, msg):
        global delivered_records
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            self.delivered_records += 1
            #print("Produced record to topic {} partition [{}] @ offset {}"
            #      .format(msg.topic(), msg.partition(), msg.offset()))

    def produce_data(self, topic, key, data):
        # Produce data to Kafka topic with given key
        record = json.dumps(data)
        #print("Producing record: {}\t{}".format(key, record))
        self.producer.produce(topic, key=key, value=record, on_delivery=self.delivery_callback)
        self.producer.poll(0)

    def flush(self):
        # Flush any remaining messages in the producer buffer
        self.producer.flush()


kproducer = KafkaProducer()


def check_output_file(f_path):
    script_path = os.path.realpath(__file__)
    file_name = os.path.basename(script_path)
    script_folder = os.path.dirname(script_path)
    data_folder = os.path.join(script_folder, 'data')

    just_file_name = os.path.splitext(os.path.basename(f_path))[0]
    matching_files = glob.glob(os.path.join(data_folder, f"{just_file_name}*.json"))

    if len(matching_files) > 0:
        f_path = os.path.join(data_folder, f"{just_file_name}_{len(matching_files) + 1}.json")

    return f_path


def get_api_response():
    response = r.get(TRIMET_API)
    return response.json()


def create_json_file(f_path, data):
    script_path = os.path.realpath(__file__)
    script_folder = os.path.dirname(script_path)
    data_folder = os.path.join(script_folder, 'data')

    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    final_file_path = os.path.join(data_folder, f_path)


    with open(final_file_path, 'w') as fo:
        json.dump(data, fo)


def send_to_kafka(data):
    for d in data:
        key = d["OPD_DATE"]
        kproducer.produce_data(KAFKA_TOPIC, key, d)
    kproducer.flush()


def fetch_data():
    #f_path = "trimet_data.json"
    #f_path = check_output_file(f_path)
    data = get_api_response()
    #create_json_file(f_path, data)
    send_to_kafka(data)
    print(f"sent total of {kproducer.delivered_records} messages")
    #print(f"File created successfully: {f_path}")


if __name__ == "__main__":
    fetch_data()
