from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_timestamp, explode, when, from_json
from pyspark.sql.types import ArrayType
import sys

class SparkStructuredStreaming:
    def __init__(self, app_name="SparkStructuredStreamingApp"):
        self.schema = """
            match_id STRING,
            timestamp STRING,
            set INT,
            game INT,
            point INT,
            server STRING,
            receiver STRING,
            server_score INT,
            receiver_score INT,
            serve_number INT,
            rally_length INT,
            event STRING,
            winner STRING,
            serve_speed_kmh DOUBLE,
            court_surface STRING,
            tournament STRING,
            round STRING,
            location STRING
        """
        
        self.spark = (
            SparkSession.builder.appName(app_name)
            .master("spark://spark-master:7077")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.network.timeout", "300s")
            .config("spark.executor.heartbeatInterval", "30s")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def read_stream(self, kafka_bootstrap_servers, topic):
        df = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load()
        )
        return df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*")

    def write_historycal_db(self, df, output_path, checkpoint_location):
        return (
            df.writeStream
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_location)
            .partitionBy("tournament", "year", "match_id")
            .outputMode("append")
            .start()
        )

    def write_kafka(self, df, kafka_bootstrap_servers, topic):
        return (
            df.selectExpr("to_json(struct(*)) AS value")
            .writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("topic", topic)
            .start()
        )
    def read_json(self, input_path):
        return self.spark.read.json(input_path, schema=self.schema)
    
if __name__ == "__main__":
    app = SparkStructuredStreaming()

    kafka_bootstrap_servers = "kafka:9092"
    input_topic = "tennis-matches"
    output_topic = "enriched-tennis-matches"
    historical_data_path = "/path/to/historical/data"
    checkpoint_location = "/path/to/checkpoint/dir"

    stream_df = app.read_stream(kafka_bootstrap_servers, input_topic)

    enriched_df = stream_df.withColumn("timestamp", to_timestamp(col("timestamp")))