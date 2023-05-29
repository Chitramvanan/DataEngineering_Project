#!/usr/bin/env python

import json
import pandas as pd
from dateutil import parser
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import psycopg2
import psycopg2.extras
from psycopg2 import sql

DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"


def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = False
	return connection


def update_Trip_table(conn, tripData):
    """
    Insert the dataframe using individual INSERT statements
    """
    cursor = conn.cursor()
    table = "trip"
    try:
        for data in tripData:
            cursor.execute(
                "UPDATE trip SET route_id = %s, service_key = %s, direction = %s WHERE trip_id = %s",
                (data["route_id"], data["service_key"], data["direction"], data["trip_id"])
            )
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Trip table is updated")
    cursor.close()

def load_data(df):
    conn = dbconnect()
    tripData = []
    for index, row in df.iterrows():
        data = {}
        data["trip_id"] = row["trip_id"]
        data["route_id"] = row["route_id"]
        data["service_key"] = row["service_key"]
        data["direction"] = row["direction"]
        tripData.append(data)
    update_Trip_table(conn, tripData)
   
# PART C - Data validation
def validateData(df):
    def calcDir(dir):
        if dir==1:
            return "Back"
        else:
            return "Out"
    def calcService(ser):
        if ser=='S':
            return "Saturday"
        elif ser=="U":
            return "Sunday"
        else:
            return "Weekday"
    df=df.dropna()
    print(df)
    df=pd.DataFrame().assign(trip_id=df["Trip id"],route_id=df["route_number"],service_key=df["service_key"],direction=df["direction"])
    df["direction"]=df["direction"].apply(calcDir)
    df["service_key"]=df["service_key"].apply(calcService)
    print(df)
    return df
        

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])
    config.update(config_parser["consumer"])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic], on_assign=reset_offset)

    
    # Process messages
    jsonData = []
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                if len(jsonData) > 0:
                    df = pd.DataFrame(jsonData)
                    df = validateData(df)
                    load_data(df)
                    jsonData.clear()
                continue
            elif msg.error():
                print("error: {}".format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                jsonData.append(data)
                #print(
                 #   "Consumed record with key {} and value {}, \
                  #    and data is {}".format(
                   #     record_key, record_value, data
                   # )
               # )
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
