#!/usr/bin/env python

import sys
import os
import json
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING


script_path = os.path.realpath(__file__)
file_name = os.path.basename(__file__)
script_folder = script_path.replace(file_name, '')
FIXED_PATH = os.path.join(script_folder, "consumed")

if not os.path.isdir(FIXED_PATH):
    os.mkdir(FIXED_PATH)

def dump_file(data):
    trip_no = data["EVENT_NO_TRIP"]
    time = data["ACT_TIME"]
    date = data["OPD_DATE"]
    #key = "%s-%s-%s.json" % (date, trip_no, time)
    key = "%s.json" % (date)
    file_path = os.path.join(FIXED_PATH, key)
    fo = open(file_path, 'a')
    json.dump(data, fo)
    fo.close()


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer(config)

    

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "sensor-data"
    consumer.subscribe([topic], on_assign=reset_offset)
    
    total_count = 0
    # Poll for new messages from Kafka and print them.
    pCount = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                total_count += 1
                dump_file(data)
                pCount = pCount + 1
                if pCount == 10000:
                   pCount = 0
                   print("Consumed record with key {} and value {}, \
                                     and updated total count to {}"
                      .format(record_key, record_value, total_count))

    except KeyboardInterrupt:
        pass
    finally:
        print('Total Records',total_count)
        # Leave group and commit final offsets
        consumer.close()
