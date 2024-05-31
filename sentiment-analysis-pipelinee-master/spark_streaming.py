import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, udf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import uuid
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("spark_structured_streaming")


def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    try:
        # Spark session is established with cassandra and kafka jars. Suitable versions can be found in Maven repository.
        spark = SparkSession \
            .builder \
            .appName("SparkStructuredStreaming") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.commons:commons-pool2:2.11.1") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.hadoop.fs.s3a.access.key", "AKIAYEQZEJ7WCXHYCDFP") \
            .config("spark.hadoop.fs.s3a.secret.key", "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") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session created successfully')
    except Exception as e:
        logging.error(f"Couldn't create the spark session: {e}")

    return spark


def create_initial_dataframe(spark_session):
    """
    Reads the streaming data and creates the initial dataframe accordingly.
    """
    try:
        # Gets the streaming data from topic random_names
        df = spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19093,kafka3:19094") \
            .option("subscribe", "reviews") \
            .option("delimiter", ",") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Initial dataframe created successfully")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    return df


def create_final_dataframe(df, spark_session):
    """
    Modifies the initial dataframe, and creates the final dataframe.
    """
    schema = StructType([
   StructField("review_id", StringType(), True),
   StructField("product", StringType()),
        StructField("review_text", StringType())
    ])
    # Initialize VADER sentiment analyzer
    analyzer = SentimentIntensityAnalyzer()

    # Define UDF for sentiment analysis
    def analyze_sentiment(text):
        print(text)
        if text is None:
            return 'neutral'
        score = analyzer.polarity_scores(text)
        if score['compound'] >= 0.05:
            return 'positive'
        elif score['compound'] <= -0.05:
            return 'negative'
        else:
            return 'neutral'

    sentiment_udf = udf(analyze_sentiment, StringType())

    def generate_uuid():
            return str(uuid.uuid4())

    uuid_udf = udf(generate_uuid, StringType())

    df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("review_id", uuid_udf()) \
            .withColumn("sentiment", sentiment_udf(col("review_text")))
    query = df.writeStream.outputMode("append").format("console").start()
    query.awaitTermination(20)

    return df


def start_streaming(df):
    logging.info("Streaming is being started...")

    try:
        query = (df.writeStream
                 .format("org.apache.spark.sql.cassandra")
                 .outputMode("append")
                 .options(table="reviews", keyspace="sentiment_analysis")
                 .option("checkpointLocation", "/tmp/checkpoints/cassandra")
                 .start())
    except Exception as e:
        logging.error(f"Error in Cassandra streaming query: {e}")

    try:
        s3_query = (df.writeStream
                    .format("csv")
                    .option("path", "s3a://real-time-sentiment-analysis-bucket/reviews")
                    .option("checkpointLocation", "/tmp/checkpoints/s3")
                    .trigger(processingTime='5 minutes')  # Increased interval
                    .start())
    except Exception as e:
        logging.error(f"Error in S3 streaming query: {e}")

    logging.info("S3 streaming QUERY STARTED !!!.")

    try:
        query.awaitTermination()
        logging.info("Cassandra streaming query terminated.")
    except Exception as e:
        logging.error(f"Cassandra streaming query error: {e}")

    try:
        s3_query.awaitTermination()
        logging.info("S3 streaming query terminated.")
    except Exception as e:
        logging.error(f"S3 streaming query error: {e}")

        
def write_streaming_data():
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df, spark)
    start_streaming(df_final)


if __name__ == '__main__':
    write_streaming_data()