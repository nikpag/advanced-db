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

firstResult="/home/user/project/times/one-worker.csv"
secondResult="/home/user/project/times/two-workers.csv"

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

mkdir -p /home/user/project/results/worker1
hdfs dfs -get /results/* /home/user/project/results/worker1

# Two workers
secondWorkerCommand="$SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 2 \
--webui-port $webUIPort --port $slaveWorkerPort --cores $cores --memory $memory spark://$masterIP:$masterSparkPort"

ssh user@snf-33421 $secondWorkerCommand

# Submit
$SPARK_HOME/bin/spark-submit \
--class $class --master spark://$masterIP:$masterSparkPort --deploy-mode $deployMode \
hdfs://$masterIP:$masterHDFSPort//jars/$jar $secondResult

mkdir -p /home/user/project/results/worker2
hdfs dfs -get /results/* /home/user/project/results/worker2

cd /home/user/project/scripts

./make-tables.sh
./extract-tables.sh

python3.8 table.py ../times/one-worker.csv ../tables/one-worker.tex
python3.8 table.py ../times/two-workers.csv ../tables/two-workers.tex
