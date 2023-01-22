#!/bin/bash

wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-{01,02,03,04,05,06}.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

hdfs dfs -mkdir -p /data/

hdfs dfs -put -f yellow* taxi* /data/

rm yellow* taxi*
