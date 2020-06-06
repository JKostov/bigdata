#!/bin/sh

docker exec -it namenode hdfs dfs -test -e /big-data
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /big-data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /big-data
fi

docker exec -it namenode hdfs dfs -test -e /big-data/data.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding data.csv in the /big-data folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /big-data/data.csv /big-data/data.csv
fi

PROJECT="$1"

if [ -z $PROJECT ]
then
  echo "[INFO]: Project argument not specified running the first big-data project with docker-compose-1.yaml"
  PROJECT="1"
fi

if [ $PROJECT != "1" -a $PROJECT != "2" -a $PROJECT != "3" -a $PROJECT != "4" ]
then
  echo "[ERROR]: Project can be: 1 or 2 or 3 or 4"
  exit 0
fi

PROJECT="docker-compose-$PROJECT.yaml"

docker-compose -f $PROJECT up
