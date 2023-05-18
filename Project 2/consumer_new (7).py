#!/usr/bin/env python

import sys
import os
import json
import pandas as pd
import datetime
import time
from datetime import datetime, timedelta
from dateutil import parser
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import psycopg2
from psycopg2 import sql
import argparse
import re
import csv
DBname = "postgres"
DBuser = "postgres"
DBpwd = "password"


# PART E - Storage : connect to the database
def dbconnect():
	connection = psycopg2.connect(
        host="localhost",
        database=DBname,
        user=DBuser,
        password=DBpwd,
	)
	connection.autocommit = False
	return connection

# load trip table
def load_Trip(conn, df):
    table = "trip"
    cols = ','.join(list(df.columns))
    placeholders = ','.join(['%s'] * len(df.columns))
    cursor = conn.cursor()

    print(f"Loading {len(df)} rows")
    for row in df.itertuples(index=False, name=None):
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (trip_id) DO NOTHING").format(
            sql.Identifier(table),
            sql.SQL(cols),
            sql.SQL(placeholders)
        )
        cursor.execute(query, row)
    
    conn.commit()

    print("Finished trip table Loading.")

#load Breadcrumb table
def load_breadcrumb(conn,df):
    table = "breadcrumb"
    cols = ','.join(list(df.columns))
    placeholders = ','.join(['%s'] * len(df.columns))
    cursor = conn.cursor()

    print(f"Loading {len(df)} rows")
    for row in df.itertuples(index=False, name=None):
        query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(cols),
            sql.SQL(placeholders)
        )
        cursor.execute(query, row)
    
    conn.commit()

    print("Finished breadcrumb table Loading.")


# PART C - Data validation
def validateData(df):
    
    cols = [
        "EVENT_NO_TRIP",
        "EVENT_NO_STOP",
        "VEHICLE_ID",
        "METERS",
        "GPS_LONGITUDE",
        "GPS_LATITUDE",
        "GPS_SATELLITES",
        "GPS_HDOP",
    ]
    df[cols] = df[cols].apply(lambda x: pd.to_numeric(x, errors="coerce"))

    df["METERS"] = pd.to_numeric(df["METERS"])

    df["EVENT_NO_TRIP"] = pd.to_numeric(df["EVENT_NO_TRIP"])

    df["EVENT_NO_STOP"] = pd.to_numeric(df["EVENT_NO_STOP"])
   

    #Ten Assertions are implemented below:
    # 1: some records are missing lat/lon values
    if (df["GPS_LONGITUDE"] == "") is None:
       # print("Assertion 1 failed: some records are missing lat/lon values")
        # df = df[df['GPS_LATITUDE'] != np.nan]
        df = df[df["GPS_LONGITUDE"] != np.nan]
    # 2: Existence assertion - Every trip must have a “trip no” associated with it.
    df = df.dropna(subset=["EVENT_NO_TRIP"])

    # 3:Inter-record assertion - The vehicle id should remain the same for a particular unique event trip no throughout the trip.
    UniqueTripIds = df["EVENT_NO_TRIP"].drop_duplicates()
    idToBusCountMap = {}
    for index, value in UniqueTripIds.items():
        uniqueVechilesCountPerId = df.loc[df["EVENT_NO_TRIP"] == value, "VEHICLE_ID"]
        idToBusCountMap[value] = uniqueVechilesCountPerId.drop_duplicates().count()

    violatingTrips = dict(
        filter(lambda elem: int(elem[1]) > 1, idToBusCountMap.items())
    )
    violationTripIds = list(violatingTrips.keys())

    df = df[~df["EVENT_NO_TRIP"].isin(violationTripIds)]

    # 4:GPS_HDOP value is always in the range 0 - 20
    def filter_by_hdop(gps_hdop):
        return float(gps_hdop) >= 0 and float(gps_hdop) <= 20

    df = df[df["GPS_HDOP"].apply(filter_by_hdop)]

    # 5:GPS_SATELLITES can never be 0, negative or empty
    def filter_by_gps_satellite_count(satellites):
        return float(satellites) > 0

    df = df[df["GPS_SATELLITES"].apply(filter_by_gps_satellite_count)]

    # 6:GPS_LONGITUDE must always be a negative value (As US is in western hemisphere) and GPS_LATITUDE must always be a positive value (As we are in northern hemisphere)
    def filter_by_GPS_LONGITUDE(longitude):
        return float(longitude) < 0

    df = df[df["GPS_LONGITUDE"].apply(filter_by_GPS_LONGITUDE)]

    def filter_by_GPS_LATITUDE(latitude):
        return float(latitude) > 0

    df = df[df["GPS_LATITUDE"].apply(filter_by_GPS_LATITUDE)]

    # 7:LIMIT ASSERTION  METERS must always be greater than equal to zero

    def filter_by_meters(mtr):
        return float(mtr) >= 0

    df = df.dropna(subset=["METERS"], how="any")

    df["METERS"] = pd.to_numeric(df["METERS"])

    df = df[df["METERS"].apply(filter_by_meters)]

    # 8:INTER-RECORD ASSERTION EVENT_NO_TRIP VALUE SHOULD ALWAYS BE LESSER THAN EVENT_NO_STOP

    def filter_by_event_no_trip(trp):
        return float(trp) >= 0

    def filter_by_event_no_stop(trp):
        return float(trp) >= 0

    def validTrip(event):
        return event["EVENT_NO_TRIP"] <= event["EVENT_NO_STOP"]

    df = df.dropna(subset=["EVENT_NO_TRIP", "EVENT_NO_STOP"], how="any")

    df["EVENT_NO_TRIP"] = pd.to_numeric(df["EVENT_NO_TRIP"])

    df["EVENT_NO_STOP"] = pd.to_numeric(df["EVENT_NO_STOP"])

    df = df[df["EVENT_NO_TRIP"].apply(filter_by_event_no_trip)]

    df = df[df["EVENT_NO_STOP"].apply(filter_by_event_no_stop)]

    df = df[df.apply(lambda row: validTrip(row), axis=1)]
    
    return df


def load_data(df):
    def getTimestamp(opd_date, act_time):
        opd_date = opd_date.split(":")[0]
        date_obj = datetime.strptime(opd_date, "%d%b%Y")
        timestamp = pd.Timestamp(date_obj) + pd.Timedelta(seconds=act_time)
        # print(timestamp)
        return timestamp
    # PART D - Data transformation
    # Sort the DataFrame by 'EVENT_NO_TRIP' column
    df = df.sort_values("EVENT_NO_TRIP")

    # Calculate the difference in distance and time between consecutive breadcrumbs
    df["dMETERS"] = df["METERS"].diff()
    df["dACT_TIME"] = df["ACT_TIME"].diff()

    # Calculate the speed for each breadcrumb
    df["SPEED"] = df["dMETERS"] / df["dACT_TIME"]
    df["SPEED"] = df["SPEED"].bfill()
    # Drop the unnecessary columns
    df = df.drop(["dMETERS", "dACT_TIME"], axis=1)

    # Print the resulting DataFrame
    #print(df.head())
    
    tripTableDf = df.drop(
        [
            "EVENT_NO_STOP",
            "METERS",
            "ACT_TIME",
           "GPS_LONGITUDE",
            "GPS_LATITUDE",
            "GPS_SATELLITES",
            "GPS_HDOP",
	    "SPEED",
        ],
        axis=1,
    )
    tripTableDf = tripTableDf.rename(columns={"EVENT_NO_TRIP": "TRIP_ID"})
    tripTableDf = tripTableDf.drop_duplicates(subset="TRIP_ID")

    tripTableDf = tripTableDf.drop(["OPD_DATE"], axis=1)

    tripTableDf["route_id"] = -1

    tripTableDf["service_key"] = None

    tripTableDf["direction"] = None

    #print(tripTableDf)

    #tripTableDf.to_csv("trip.csv", index=False)

    breadCrumbDf = df.drop(
        ["EVENT_NO_STOP", "VEHICLE_ID", "METERS", "GPS_SATELLITES", "GPS_HDOP"], axis=1
    )
    breadCrumbDf = breadCrumbDf.rename(
        columns={
            "EVENT_NO_TRIP": "TRIP_ID",
            "GPS_LONGITUDE": "longitude",
            "GPS_LATITUDE": "latitude",
        }
    )


    breadCrumbDf["ACT_TIME"] = breadCrumbDf.apply(
        lambda row: getTimestamp(row["OPD_DATE"], row["ACT_TIME"]), axis=1
    )
    breadCrumbDf = breadCrumbDf.drop(["OPD_DATE"], axis=1)

    breadCrumbDf = breadCrumbDf.rename(columns={"ACT_TIME": "tstamp"})
    print(breadCrumbDf)
    #breadCrumbDf.to_csv("breadcrumb.csv", index=False)

    conn = dbconnect()
    load_Trip(conn,tripTableDf)
    load_breadcrumb(conn,breadCrumbDf)

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
