#!/bin/bash

masterIP="192.168.0.1"
masterWorkerPort="65509"
slaveWorkerPort="65510"
webUIPort="8080"
masterSparkPort="7077"
masterHDFSPort="9000"

cores="8"
memory="8g"

jar="main_2.12-1.0.jar"
class="Main"
deployMode="client"

firstResult="/home/user/project/results/first_result"
secondResult="/home/user/project/results/second_result"

# Build Scala, put jar to hdfs
cd ~/project/scala/ 
~/sbt/bin/sbt package
hdfs dfs -put -f /home/user/project/scala/target/scala-2.12/$jar /jars/$jar

# One worker
spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 \
--webui-port $webUIPort --port $masterWorkerPort --cores $cores --memory $memory spark://$masterIP:$masterSparkPort

# Submit
$SPARK_HOME/bin/spark-submit \
--class $class --master spark://$masterIP:$masterSparkPort --deploy-mode $deployMode \
hdfs://$masterIP:$masterHDFSPort//jars/$jar $firstResult