#!/bin/bash

# Download and extract apache spark
wget https://downloads.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop2.7.tgz
tar -xzf spark-3.1.3-bin-hadoop2.7.tgz

# Set Spark environment variables
echo "SPARK_HOME=/home/user/spark-3.1.3-bin-hadoop2.7" >> ~/.bashrc
echo 'PATH=$PATH:$SPARK_HOME/sbin' >> ~/.bashrc
source ~/.bashrc

# Install correct java version
sudo apt install openjdk-8-jdk
java -version # Expected output: openjdk version "1.8.0_292"

# Set SPARK_MASTER_HOST
touch ~/spark-3.1.3-bin-hadoop2.7/conf/spark-env.sh
echo "SPARK_MASTER_HOST='192.168.0.1'" >> ~/spark-3.1.3-bin-hadoop2.7/conf/spark-env.sh

start-master.sh
start-my-workers.sh # Custom script