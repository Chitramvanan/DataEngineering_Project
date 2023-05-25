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
KAFKA_TOPIC = "purchases"


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

    def produce_data(self, topic, key, data):
        # Produce data to Kafka topic with given key
        record = json.dumps(data)
        self.producer.produce(topic, key=key, value=record, on_delivery=self.delivery_callback)
        self.producer.poll(0)

    def flush(self):
        # Flush any remaining messages in the producer buffer
        self.producer.flush()


kproducer = KafkaProducer()


def get_api_response():
    response = r.get(TRIMET_API)
    return response.json()
    # with open('out.json', 'r') as file:
    #     data = json.load(file)
    # return data


def send_to_kafka(data):
    counter = 0
    for d in data:
        counter += 1
        key = d["OPD_DATE"]
        kproducer.produce_data(KAFKA_TOPIC, key, d)
        if counter == 1000:
            kproducer.flush()
            counter = 0
    kproducer.flush()


def fetch_data():
    data = get_api_response()
    send_to_kafka(data)
    print(f"sent total of {kproducer.delivered_records} messages")


if __name__ == "__main__":
    fetch_data()
