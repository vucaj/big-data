#!/bin/bash

docker cp ../batch/ spark-master:/home

docker exec -it spark-master bash -c "cd home/batch && /spark/bin/spark-submit /home/batch/transformation.py"
docker exec -it spark-master bash -c "cd home/batch && /spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 currated.py"