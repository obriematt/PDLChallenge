#!/user/bin/env python

import sys
import os
import gzip
import json
from kafka import KafkaClient, KafkaProducer, KafkaConsumer
from json import dumps
from time import sleep
import time
from kafka.metrics import MetricName, Metrics, AnonMeasurable, KafkaMetric
from kafka.metrics.stats import Sensor
import csv

# Checking for folder path, topic name to add, and bootstrap server
if not sys.argv[1]:
	sys.exit("No folder path given.")
else:
	folder_location = sys.argv[1]

if not sys.argv[2]:
	sys.exit("No topic name given")
else:
	topic = sys.argv[2] 

if not sys.argv[3]:
	sys.exit("No bootstrap_servers given")
else:
	bootstrap_server = sys.argv[3]

print(folder_location, topic, bootstrap_server)

try:
	producer = KafkaProducer(bootstrap_servers=[bootstrap_server],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
except:
	print("Failed to create KafkaProducer")


start_folder = time.time()

total_lines = 0
# Attempt to open directory, files, and send to kafka
try:
	for file in os.listdir(folder_location):
		unzip_file = gzip.open(folder_location+'/'+file, 'rU')
		otherReader = csv.DictReader(unzip_file)
		out = json.dumps( [ row for row in otherReader ], sort_keys=True, indent=4).encode('utf-8')  
		parsed = json.loads(out)

		for line in parsed:
			producer.send(topic, value=line)
			total_lines = total_lines + 1
except:
	print("failed to send data to kafka server.")


end_folder = time.time()
total_time = end_folder-start_folder
lines_per_second = total_lines / total_time

metrics = producer.metrics()
metrics_file = open("benchmark.txt", "a")

print('Record send rate per second: {metric}'.format(metric=metrics['producer-topic-metrics.{topic}'.format(topic=topic)]['record-send-rate']))
print('Incoming byte rate: {metric}'.format(metric=metrics['producer-node-metrics.node-0']['incoming-byte-rate']))
print('Total records sent per second: {metric}'.format(metric=metrics['producer-topic-metrics.{topic}'.format(topic=topic)]['record-send-rate']))
print('{total_lines} lines parsed and sent to kafka in {total_time}'.format(total_time=total_time, total_lines=total_lines))
print('{lines_per_second} lines per second'.format(lines_per_second=lines_per_second))

try:
	metrics_file = open("benchmark.txt", "a")
	metrics_file.write('Record send rate per second: {metric}'.format(metric=metrics['producer-topic-metrics.{topic}'.format(topic=topic)]['record-send-rate']))
	metrics_file.write('Incoming byte rate: {metric}'.format(metric=metrics['producer-node-metrics.node-0']['incoming-byte-rate']))
	metrics_file.write('Total records sent per second: {metric}'.format(metric=metrics['producer-topic-metrics.{topic}'.format(topic=topic)]['record-send-rate']))
	metrics_file.write('{total_lines} lines parsed and sent to kafka in {total_time}'.format(total_time=total_time, total_lines=total_lines))
	metrics_file.write('{lines_per_second} lines per second'.format(lines_per_second=lines_per_second))
except:
	print("failed to write benchmark.txt")