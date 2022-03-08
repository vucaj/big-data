#!/bin/bash

docker cp datasets/ namenode:/home/

docker exec -it namenode bash -c "hdfs dfs -mkdir /videos"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/CA_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/CAvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/DE_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/DEvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/FR_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/FRvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/GB_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/GBvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/IN_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/INvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/JP_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/JPvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/KR_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/KRvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/MX_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/MXvideos.csv /videos/"

docker exec -it namenode bash -c "hdfs dfs -put home/datasets/US_category_id.json /videos/"
docker exec -it namenode bash -c "hdfs dfs -put home/datasets/USvideos.csv /videos/"

