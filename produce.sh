#!/bin/bash

echo " "
echo ">>>> removing files"

#if $(test -d ./movie_titles) ; then rm -rf ./movie_titles; fi
#if $(test -d ./netflix-prize-data) ; then rm -rf ./netflix-prize-data; fi
#if $(test ./KafkaProducer.jar) ; then rm  ./KafkaProducer.jar; fi

echo " "
echo ">>>> preparing files"

#mkdir -p ./movie_titles
#hadoop fs -copyToLocal gs://bd-bucket-uniq-0/movie_titles.csv ./movie_titles/movie_titles.csv
#
#hadoop fs -copyToLocal gs://bd-bucket-uniq-0/netflix-prize-data-10.zip ./netflix-prize-data.zip
#unzip ./netflix-prize-data.zip
#rm ./netflix-prize-data.zip
#
#hadoop fs -copyToLocal gs://bd-bucket-uniq-0/KafkaProducer.jar ./KafkaProducer.jar

echo " "
echo ">>>> creating kafka topics for input data"

kafka-topics.sh --create \
 --zookeeper localhost:2181 \
 --replication-factor 1 --partitions 1 --topic kafka-movie-titles
kafka-topics.sh --create \
 --zookeeper localhost:2181 \
 --replication-factor 1 --partitions 1 --topic kafka-netflix-prize-data
kafka-topics.sh --create \
 --zookeeper localhost:2181 \
 --replication-factor 1 --partitions 1 --topic kafka-prize-alerts

echo " "
echo ">>>> loading movies titles"

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer movie_titles 0 kafka-movie-titles \
 1 ${CLUSTER_NAME}-w-0:9092

echo " "
echo ">>>> running kafka producer for netflix prize data"

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar \
 com.example.bigdata.TestProducer netflix-prize-data 0 kafka-netflix-prize-data \
 1 ${CLUSTER_NAME}-w-0:9092
