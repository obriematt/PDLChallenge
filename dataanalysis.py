#!/user/bin/env python

import os
from pyspark import SparkContext
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime
from json import dumps
from dataschema import myschema

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0 pyspark-shell'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 pyspark-shell'

# Spark session
try:
	spark = SparkSession.builder.appName("Data analysis").getOrCreate()
except:
	print("Failed to start spark session")

# Spark Context, reading and loading JSON values into dataframe
try:
	df = spark.read.format("kafka").option("startingOffsets", "earliest").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "raw_data").load().selectExpr("CAST(value AS STRING)")
	dfJSON = df.select(from_json("value", myschema).alias("tickets"))
except:
	print("Failed to read data from kafka.")

# This will group the data set by months for seasonality. Showing from December -> January
try:
	dfMonthAgg = dfJSON.select('*', to_timestamp(dfJSON['tickets']['Issue Date'], 'MM/dd/yyyy').alias('dt'))
	toWriteMonthDF = dfMonthAgg.groupBy(month('dt').alias('months')).count().sort(desc('months'))
	toWriteMonthDF.repartition(1).write.option("header", "true").csv("Tickets-per-month.csv")
except:
	print("Failed to analyze and write the Tickets per month data.")

# Getting location of tickets. Via Violation Location in desc order by ticket count
try:
	dfLocationAgg = dfJSON.groupby(dfJSON['tickets']['Violation Location'].alias('Location')).count().sort(desc('count'))
	dfLocationAgg.repartition(1).write.option("header", "true").csv("location-tickets.csv")
except:
	print("Failed to analyze and write the Violation location data.")

# Getting count of years/type, sort by desc order of tickets.
try:
	dfYearTypeAgg = dfJSON.groupby(dfJSON['tickets']['Vehicle Make'], dfJSON['tickets']['Vehicle Year']).count().sort(desc('count'))
	dfYearTypeAgg.repartition(1).write.option("header", "true").csv("car-year-type-tickets.csv")
except:
	print("Failed to analyze and write the Car Type and Year ticket data.")