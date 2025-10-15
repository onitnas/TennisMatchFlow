from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from fastparquet import ParquetFile

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
        
        self.spark = SparkSession.builder.appName(app_name) \
        .master("spark://spark-master:7077") \
        .getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        
    def read_stream(self, kafka_bootstrap_servers, topic):
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()
            
        return df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), self.schema).alias("data")) \
            .select("data.*")
    
    def write_historycal_db(self, df, output_path, checkpoint_location):
        df \
            .writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_location) \
            .partitionBy("tournament", "year", "match_id") \
            .outputMode("append") \
            .start()
    
    
    
    def write_kafka(self, df, kafka_bootstrap_servers, topic):
        df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", topic) \
            .start()
    
    
if __name__ == "__main__":
    kafka_bootstrap_servers = "kafkaServer:9092"
    topic = "new_point"
    output_path = "/data/spark/matches/parquet/"
    checkpoint_location = "/data/spark/matches/checkpoint/"
    
    streaming_app = SparkStructuredStreaming()
    df = streaming_app.read_stream(kafka_bootstrap_servers, topic)
    streaming_app.write_historycal_db(df, output_path, checkpoint_location)