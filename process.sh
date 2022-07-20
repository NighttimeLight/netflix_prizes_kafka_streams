#!/bin/bash

echo " "
echo ">>>> running kafka streams processing"

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp /usr/lib/kafka/libs/*:npd_kafka_streams_processing.jar \
 com.example.bigdata.NpdProcessing ${CLUSTER_NAME}-w-0:9092 daily 1 2 3