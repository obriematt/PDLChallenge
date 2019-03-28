#!/bin/bash

echo "Installing pip"
sudo apt install python3-pip
echo "Installing AWS CLI"
pip install awscli --upgrade --user
echo "Installing Kafka/Zookeeper"
echo "Updating apt-get"
sudo apt-get update
echo "Installing/checking for necessary java"
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get install oracle-java8-installer -y
echo "Intalling Zookeeper"
sudo apt-get install zookeeperd
echo "Installing Kafka"
wget https://www-us.apache.org/dist/kafka/2.1.0/kafka_2.12-2.1.0.tgz
mkdir ~/kafka
sudo tar -xvf kafka_2.12-2.1.0.tgz -C ~/kafka/
echo "Finished installing. Kafka/Zookeeper located at ~/kafka"
