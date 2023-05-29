#!/usr/bin/env python

import json
import requests
import re
from bs4 import BeautifulSoup
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer


if __name__ == '__main__':
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
    producer = Producer(config)
    delivered_records = 0

    def delivery_callback(err, msg):
        global delivered_records
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            delivered_records += 1

    
    url = "http://www.psudataeng.com:8000/getStopEvents"
    topic = "purchases"
    
    html_text = requests.get(url).text
    soup = BeautifulSoup(html_text, "html.parser")

    
    
    # Get all trip ids
    def getTripNumberRegex(str):
        return re.findall(r'\d+', str)
    
    trip_ids = []
    heading_tags = ["h2"]
    for tags in soup.find_all(heading_tags):
        trip_ids.append(getTripNumberRegex(tags.text.strip())[0])
    
    trimet_tables = soup.find_all("table")
    trimet_table_data = trimet_tables[0].find_all("tr")
    
    # Get all the headings of Lists
    headings = []
    for th in trimet_table_data[0].find_all("th"):
        # remove any newlines and extra spaces from left and right
        headings.append(th.get_text().replace('\n', ' ').strip())
        
        
    data = {}
    for i in range(len(trip_ids)):
        trip_id = trip_ids[i]
        trimet_table = trimet_tables[i]
        trimet_table_data = trimet_table.find_all("tr")
        
        table_data = []
        for i in range(1, len(trimet_table_data)):
            
            t_row = {}
            t_row["Trip id"] = trip_id 
            # find all td's(3) in tr and zip it with t_header
            for td, th in zip(trimet_table_data[i].find_all("td"), headings): 
                t_row[th] = td.text.replace('\n', '').strip()
            table_data.append(t_row)
        data[trip_id] = table_data
        
    entries = []
    for i in range(len(trip_ids)):
        trip_data_array = data[trip_ids[i]]
        for i in range(len(trip_data_array)):
            entries.append(trip_data_array[i])
    
    
    for i in range(len(entries)):
        n = json.dumps(entries[i])
        producer.produce(topic, n, on_delivery=delivery_callback)
        producer.poll(0)
    
    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))

    