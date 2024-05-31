#!/bin/bash

# Copy the spark_streaming.py file to the spark_master container
docker cp spark_streaming.py spark_master:/opt/bitnami/spark/

# Install the vaderSentiment package in the spark_master container
docker exec -u 0 -it spark_master /bin/bash -c "pip install vaderSentiment"

# Download the commons-pool2 jar file into the spark_master container
docker exec -u 0 -it spark_master /bin/bash -c "
cd /opt/bitnami/spark/jars && \
curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
"

# Submit the Spark job
# docker exec -u 0 -it spark_master /bin/bash -c "
# spark-submit --master local[2] --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901,org.apache.hadoop:hadoop-common:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar /opt/bitnami/spark/spark_streaming.py
# "
