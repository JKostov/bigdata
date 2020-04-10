#!/bin/sh

docker kill submit
docker rm submit
docker rmi big-data_submit

docker exec -it namenode hdfs dfs -test -e /big-data
if [ $? -eq 1 ]
then
  echo "Creating /big-data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /big-data
fi

docker exec -it namenode hdfs dfs -test -e /big-data/data.csv
if [ $? -eq 1 ]
then
  echo "Adding data.csv in the /big-data folder on the HDFSz"
  docker exec -it namenode hdfs dfs -put /big-data/data.csv /big-data/data.csv
fi

docker-compose up -d