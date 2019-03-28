#!/user/bin/env python

import os
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from kafka import KafkaProducer
from json import dumps
from dataschema import myschema

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'

try:
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
	                         value_serializer=lambda x: 
	                         dumps(x).encode('utf-8'))
except:
	print("Failed to start kafka producer")

# Spark session
try:
	spark = SparkSession.builder.appName("Year Filter App").getOrCreate()
except:
	print("Failed to start spark session")

# Spark Context, reading and loading JSON values into dataframe
try:
	df = spark.read.format("kafka").option("startingOffsets", "earliest").option("kafka.bootstrap.servers", 'localhost:9092').option("subscribe", 'raw_data').load().selectExpr("CAST(value AS STRING)")
	dfTwo = df.select(from_json("value", myschema).alias("tickets"))

	# Creating new dataframe of only 2014 tickets, and stripping off extra column used for comparison
	dfThree = dfTwo.select('*', to_timestamp(dfTwo['tickets']['Issue Date'], 'MM/dd/yyyy').alias('dt')).where(year(col('dt'))==2014)
	nextDF = dfThree.drop(col('dt'))
except:
	print("Failed to gather filtered year data.")

# Write new dataframe as json to new topic.
try:
	(nextDF.select(to_json(struct([nextDF[x] for x in nextDF.columns])).alias("value"))
	    .write
	    .format("kafka")
	    .option("kafka.bootstrap.servers", 'localhost:9092')
	    .option("topic", 'filtered_data')
	    .save())
except:
	print("Failed to save the new topic of filtered data")
