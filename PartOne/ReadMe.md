# Data Engineering Challenge Part 2.1.2 Files.

Steps to Ingest data in Kafka. I chose to use command line inputs due to the fact that the folder location of the data was necessary.
1. Run the aws_cli_kafka_installation bash file within terminal.
2. Navigate to the ~/kafka directory created.
3. Start the kafka server with bin/kafka-server-start.sh config/server.properties
4. In a separate terminal, navigate to the ~/kafka directory
5. Run the script bin/zookeeper-server-start.sh config/zookeeper.properties. Note: Zookeeper automatically starts when installed. To start a different process change the port to 5181.
6. Using AWS CLI download the data from s3://open.peoplelabs.com/data_engineer_challenge/
7. This can be accomplished by configuring aws cli, and using "aws s3 sync s3://open.peoplelabs.com/data_engineer_challenge/" via command line.
8. Run the python script: python commandlineproducer.py 'location of download files' 'topic name ie raw_data' 'bootstrap server ie localhost:9092' from the PDLChallenge project directory.

The script will Ingest the csv files into a local kafka server topic with the input given as JSON objects.
The text file version of the scripts are located within "kafka_client.txt" and the bench metrics are located within "benchmark.txt".

