# advdb-ntua
Project for "Advanced Topics in Database Systems" course @ NTUA (9th semester)

## Μέλη Ομάδας (advdb37):
 - Βασίλης Βρεττός el18126
 - Νίκος Παγώνας el18175

# Installation
Στην εργασία, χρησιμοποιήσαμε δύο συστήματα, το Apache Spark (σε cluster mode) και το DFS μέρος των εργαλείων Hadoop.
Και τα δύο συστήματα εγκαταστάθηκαν και στα δύο μηχανήματα που μας δόθηκαν ως πόροι για την εκτέλεση της εργασίας.

Οι παρακάτω οδηγίες εγκατάστασης υπάρχουν και σε ξεχωριστά scripts στον ομώνυμο φάκελο του repository.

## Apache Spark Installation
Εκτελούμε τις παρακάτω εντολές και στα 2 μηχανήματα

**Σημείωση**: Στο slave μηχάνημα, δεν χρειάζεται να τρέξουμε τις εντολές που κάνουν set την IP του master node και **ΔΕΝ** πρέπει να τρέξουμε την εντολή ```start-master.sh```. Για την εγκατάσταση του Apache Spark στα 2 nodes, υπάρχουν τα scripts ```install_spark_master.sh``` και ```install_spark_slave.sh``` αντίστοιχα.

```bash
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
```

## HDFS Installation
Η εγκατάσταση του HDFS είναι αρκετά πιο δύσκολη για να αυτοματοποιηθεί με scripts και για αυτό περιγράφουμε συνοπτικά την διαδικασία. [Ακολουθήσαμε αυτό το guide](https://sparkbyexamples.com/hadoop/apache-hadoop-installation/) με πολύ καλά αποτελέσματα.

### Steps for both master and worker node
```bash
# Setup passwordless SSH
slaveIP = '192.168.0.2'
sudo apt-get install ssh
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat .ssh/id_rsa.pub >> ~/.ssh/authorized_keys
scp .ssh/authorized_keys $slaveIP:/home/ubuntu/.ssh/authorized_keys
```

Αναγνωρίζουμε και τα 2 μηχανήματα ως known hosts μεταξύ τους.

```console
<!-- Edit Known Hosts document -->
sudo vi /etc/hosts
127.0.0.1       localhost
192.168.0.1     snf-33420 
192.168.0.2     snf-33421
```

```bash
masterIP = '192.168.0.1'
slaveIP = '192.168.0.2'

# Download and extract Apache Hadoop
wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz

tar -xzf hadoop-2.7.0.tar.gz
mv hadoop-2.7.0 hadoop

# Setup hadoop environmental variables
echo 'HADOOP_HOME=/home/user/hadoop' >> ~/.bashrc
echo 'PATH=$PATH:$HADOOP_HOME/bin' >> ~/.bashrc
echo 'PATH=$PATH:$HADOOP_HOME/sbin' >> ~/.bashrc
echo 'HADOOP_MAPRED_HOME=${HADOOP_HOME}' >> ~/.bashrc
echo 'HADOOP_COMMON_HOME=${HADOOP_HOME}' >> ~/.bashrc
echo 'HADOOP_HDFS_HOME=${HADOOP_HOME}' >> ~/.bashrc
echo 'YARN_HOME=${HADOOP_HOME}' >> ~/.bashrc
echo 'JAVA_HOME=$(readlink -f $(which java) | sed s:/jre/bin/java::g)' >> ~/.bashrc
source ~/.bashrc

# Copy xml configuration files from repository folder to hadoop /etc folder
cp scripts/hdfs_config/* $HADOOP_HOME/etc/hadoop/

# Create HDFS Data folder
sudo mkdir -p /usr/local/hadoop/hdfs/data
sudo chown user:user -R /usr/local/hadoop/hdfs/data
chmod 700 /usr/local/hadoop/hdfs/data

# Create masters/workers/slaves file
printf "$masterIP" > $HADOOP_HOME/etc/hadoop/masters
printf "$masterIP\n$slaveIP" > $HADOOP_HOME/etc/hadoop/workers
printf "$masterIP\n$slaveIP" > $HADOOP_HOME/etc/hadoop/slaves
```

### Extra steps for Master Node
```bash
# Format HDFS and start it
hdfs namenode -format
start-yarn.sh
start-dfs.sh
```

# Φόρτωμα αρχείων parquet/csv
Κατεβάζουμε τα parquet files που απαιτούνται για να τρέξουμε τα ερωτήματα καθώς και το lookup αρχέιο των περιοχών και τα τοποθετούμε στο HDFS. Τις ενέργειες αυτές εκτελούνται από το script ```load-data.sh```

```bash
#!/bin/bash

wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{01,02,03,04,05,06}.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

hdfs dfs -mkdir -p /data/

hdfs dfs -put -f yellow* taxi* /data/

rm yellow* taxi*
```
# Build/εκτέλεση κώδικα

Σαν γλώσσα προγραμματισμού επιλέξαμε την Scala λόγω της μεγάλης συμβατότητας που έχει με το Spark. Ως build tool χρησιμοποιούμε το SBT. Η εγκατάστασή του είναι σύντομη και ως εξής:

```bash
# Download and extract SBT
https://github.com/sbt/sbt/releases/download/v1.8.2/sbt-1.8.2.tgz
tar -xzf sbt-1.8.2.tgz
```

Δημιουργούμε νέο Scala Project, με ειδικό template, με την εντολή
```bash
mkdir advDBProject && cd advDBProject
~/sbt/bin/sbt new scala/hello-world.g8
```

Στο αρχείο ```advDBProject/src/main/scalaMain.scala``` πλέον γράφουμε τον κώδικα του project.

Για να εκτελέσουμε τον κώδικα πρέπει να γίνει compiled και να τοποθετηθεί στο HDFS.

```bash
# Build Scala, put jar to hdfs
cd ~/project/scala/ 
~/sbt/bin/sbt package
hdfs dfs -put -f /home/user/project/scala/target/scala-2.12/$jar /jars/$jar
```

Πλέον, μπορεί να εκτελεστεί με όσα workers έχουμε φορτώσει στο σύστημα του Spark

```bash
$SPARK_HOME/bin/spark-submit \
--class $class --master spark://$masterIP:$masterSparkPort --deploy-mode $deployMode \
hdfs://$masterIP:$masterHDFSPort//jars/$jar $firstResult
```

**Σημείωση**: Τα scripts είναι παραμετροποιημένα. Ολόκληρο το pipeline εκτελείται από τα scripts ```run.sh``` και ```run_one_worker.sh```


```bash
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
```