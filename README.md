# Information


This project demonstrates the use of Apache Spark Structured Streaming with Kafka, Cassandra, and S3. It processes streaming data from Kafka, performs sentiment analysis using VADER, and stores the results in a Cassandra database. Additionally, it archives the processed data to S3 in CSV format. Airflow is used to trigger the review submission process.

Project Structure
stream_to_kafka.py: A Flask app that serves as the UI for submitting reviews.
spark_streaming.py: A Spark Structured Streaming job that reads data from Kafka, performs sentiment analysis, and writes the results to Cassandra and S3.
docker-compose.yml: A Docker Compose file to set up the necessary containers for Kafka, Zookeeper, Cassandra, Spark, and the Flask app.
kafka_review_submission_dag.py: An Airflow DAG to trigger the review submission process.
requirements.txt: Python dependencies for the Flask app.
Dockerfile: Dockerfile for building the Flask app container.

## Architecture:
<img width="1419" alt="image" src="architecture.png">

## Detailed Explanation of Each Component
Flask App (stream_to_kafka.py)
This Flask app provides a simple web interface for users to submit reviews. It sends the submitted reviews to a Kafka topic named reviews.

### Key Functions:

index(): Renders the main page with a form to submit reviews.
submit_review(): Receives the review data and sends it to Kafka.
Spark Streaming Job (spark_streaming.py)
This script defines a Spark Structured Streaming job that reads review data from Kafka, performs sentiment analysis using VADER, writes the results to Cassandra, and archives the data to S3.


create_spark_session(): Sets up the Spark session with necessary configurations.
create_initial_dataframe(): Reads streaming data from Kafka and creates an initial DataFrame.
create_final_dataframe(): Applies transformations to the initial DataFrame, including sentiment analysis.
start_streaming(): Defines and starts the streaming queries to Cassandra and S3.
Airflow DAG (dags/kafka_review_submission_dag.py)
This DAG triggers the review submission process using a Python script. It runs periodically or can be triggered manually.

### Key Tasks:

submit_review_to_kafka: A task that triggers the submit_review.py script to send a review to Kafka.
Docker Compose (docker-compose.yml)
This file defines the Docker services for Kafka, Zookeeper, Cassandra, Spark, the Flask app, and Airflow. It ensures that all necessary components are started and configured correctly.

### Key Services:

webapp: The Flask app service.
zoo1: Zookeeper service.
kafka1, kafka2, kafka3: Kafka broker services.
cassandra: Cassandra database service.
spark: Spark service.
kafka-ui: Kafka UI service.
airflow-scheduler, airflow-webserver, airflow-worker, airflow-init: Airflow services.


## Apache Airflow

Run the following command to clone the necessary repo on your local

```bash
git clone https://github.com/dogukannulu/docker-airflow.git
```
After cloning the repo, run the following command only once:

```bash
docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .
```

Then change the docker-compose-LocalExecutor.yml file with the one in this repo and add `requirements.txt` file in the folder. This will bind the Airflow container with Kafka and Spark container and necessary modules will automatically be installed:

```bash
docker-compose -f docker-compose-LocalExecutor.yml up -d
```
```bash
docker exec -u 0 -it docker-airflow-webserver-1 /bin/bash
pip install cassandra-driver boto3 pandas
```


Now you have a running Airflow container and you can access the UI at `https://localhost:8080`

## Apache Kafka

`docker-compose.yml` will create a multinode Kafka cluster. We can define the replication factor as 3 since there are 3 nodes (kafka1, kafka2, kafka3). We can also see the Kafka UI on `localhost:8888`. 

We should only run:

```bash
docker-compose up -d
```

<img width="1419" alt="image" src="https://github.com/dogukannulu/kafka_spark_structured_streaming/assets/91257958/2b55fbc9-d0f8-4919-880e-58c7f68c653d">




After accessing to Kafka UI(u should wait a few minutes ), we can create the topic `reviews`.so, we can see the messages coming to Kafka topic:






## Cassandra
`docker-compose.yml` will also create a Cassandra server. Every env variable is located in `docker-compose.yml`. I also defined them in the scripts.

By running the following command, we can access to Cassandra server:

```bash
docker exec -it cassandra /bin/bash
```

After accessing the bash, we can run the following command to access to cqlsh cli.

```bash
cqlsh -u cassandra -p cassandra
```

Then, we can run the following commands to create the keyspace `sentient_analysis` and the table `reviews`:

```bash
CREATE KEYSPACE IF NOT EXISTS sentiment_analysis WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS sentiment_analysis.reviews (
  review_id UUID PRIMARY KEY,
  product TEXT,
  review_text TEXT,
  sentiment TEXT
);
```







## Running DAGs

We should move `stream_to_kafka_dag.py` scripts under `dags` folder in `docker-airflow` repo and create a folder `scripts` and move the script `submit_review.py` there. Then we can see that the dag appears in DAGS page.


When we turn the OFF button to ON, the reviews submitted to the webapp will be sent to the kafka topic.

## Flask webapp
We can access to the webapp by running http://localhost:5000 to submit our reviews



## Spark
First of all we should copy the local PySpark script into the container:

```bash
docker cp spark_streaming.py spark_master:/opt/bitnami/spark/
```
We shoud install the sentiment analysis library in Spark
```bash
docker exec -u 0 -it spark_master /bin/bash
pip install vaderSentiment
exit
```

We should then access the Spark container and install necessary JAR files under jars directory.

```bash
docker exec -it spark_master /bin/bash
```

We should run the following commands to install the necessary JAR files under for Spark version 3.3.0:

```bash
cd jars
curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
```

While the APP data is sent to the Kafka topic `reviews` every time we trigger the dag, we can submit the PySpark application and write the topic data to Cassandra table and S3 bucket:

```bash
cd ..
spark-submit --master local[2] --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901,org.apache.hadoop:hadoop-common:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --jars /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar spark_streaming.py
```

After running the commmand, we can see that the data is populated into Cassandra table
and find the csv in the bucket 


Enjoy :)

