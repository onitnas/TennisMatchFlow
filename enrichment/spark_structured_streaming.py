# enrichment/spark_structured_streaming.py

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, to_timestamp, expr, count, avg, max as Fmax, from_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)


class SparkStructuredStreaming:
    """
    Pipeline completa:
      - Legge eventi da Kafka (topic input)
      - Scrive storico in Parquet (Query A)
      - Arricchisce per-batch confrontando con snapshot Parquet
      - Scrive arricchito su Kafka (topic output) (Query B)
    """

    def __init__(
        self,
        app_name: str = "TennisStreamEnrichment",
        spark_master: str = "spark://spark-master:7077",
        kafka_bootstrap: str = "kafkaServer:9092",
        input_topic: str = "new_point",
        output_topic: str = "data_enriched",
        parquet_path: str = "/data/spark/matches/parquet",
        checkpoint_hist: str = "/data/spark/checkpoints/historical",
        checkpoint_enr: str = "/data/spark/checkpoints/enrichment",
        starting_offsets: str = "latest",
        network_timeout: str = "300s",
        heartbeat_interval: str = "30s",
    ):
        # ---- Config ----
        self.app_name = app_name
        self.spark_master = spark_master
        self.kafka_bootstrap = kafka_bootstrap
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.parquet_path = parquet_path
        self.checkpoint_hist = checkpoint_hist
        self.checkpoint_enr = checkpoint_enr
        self.starting_offsets = starting_offsets
        self.network_timeout = network_timeout
        self.heartbeat_interval = heartbeat_interval

        # ---- Spark ----
        self.spark = (
            SparkSession.builder
            .appName(self.app_name)
            .master(self.spark_master)
            .config("spark.network.timeout", self.network_timeout)
            .config("spark.executor.heartbeatInterval", self.heartbeat_interval)
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")

        # ---- Schema ----
        self.schema = self._event_schema()

        # ---- Query handlers (per stop/monitoring) ----
        self.q_hist = None
        self.q_enr = None

    # =========================
    # Schema
    # =========================
    @staticmethod
    def _event_schema() -> StructType:
        return StructType([
            StructField("match_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("set", IntegerType()),
            StructField("game", IntegerType()),
            StructField("point", IntegerType()),
            StructField("server", StringType()),
            StructField("receiver", StringType()),
            StructField("server_score", IntegerType()),
            StructField("receiver_score", IntegerType()),
            StructField("serve_number", IntegerType()),
            StructField("rally_length", IntegerType()),
            StructField("event", StringType()),
            StructField("winner", StringType()),
            StructField("serve_speed_kmh", DoubleType()),
            StructField("court_surface", StringType()),
            StructField("tournament", StringType()),
            StructField("round", StringType()),
            StructField("location", StringType()),
        ])

    # =========================
    # Source (Kafka)
    # =========================
    def _read_stream(self) -> DataFrame:
        raw = (
            self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_bootstrap)
                .option("subscribe", self.input_topic)
                .option("startingOffsets", self.starting_offsets)
                .load()
        )
        events = (
            raw.selectExpr("CAST(value AS STRING) AS json_str")
               .select(from_json(col("json_str"), self.schema).alias("data"))
               .select("data.*")
               .withColumn("timestamp", to_timestamp("timestamp"))
               .withColumn("year", year("timestamp"))
        )
        return events

    # =========================
    # Writer storico (Parquet) - Query A
    # =========================
    def _start_historical_query(self, df: DataFrame):
        self.q_hist = (
            df.writeStream
              .format("parquet")
              .option("path", self.parquet_path)
              .option("checkpointLocation", self.checkpoint_hist)
              .partitionBy("tournament", "year", "match_id")
              .outputMode("append")
              .start()
        )
        return self.q_hist

    # =========================
    # Enrichment helpers
    # =========================
    def _load_history_snapshot(self) -> Optional[DataFrame]:
        try:
            return self.spark.read.parquet(self.parquet_path)
        except Exception:
            return None

    def _enrich_with_history(self, batch_df: DataFrame, history_df: Optional[DataFrame]) -> DataFrame:
        cur = (
            batch_df
            .withColumn("ts", to_timestamp("timestamp"))
            .withColumn("year", year("timestamp"))
        )

        if history_df is None:
            # Primo batch senza storico
            return (
                cur.withColumn("server_total_points_hist", expr("CAST(NULL AS BIGINT)"))
                   .withColumn("server_avg_speed_hist", expr("CAST(NULL AS DOUBLE)"))
                   .withColumn("server_last_seen_ts", expr("CAST(NULL AS TIMESTAMP)"))
                   .withColumn("receiver_total_points_hist", expr("CAST(NULL AS BIGINT)"))
                   .withColumn("receiver_avg_rally_hist", expr("CAST(NULL AS DOUBLE)"))
                   .withColumn("receiver_last_seen_ts", expr("CAST(NULL AS TIMESTAMP)"))
            )

        hist = history_df.withColumn("ts", to_timestamp("timestamp"))

        server_hist = (
            hist.groupBy("server")
                .agg(
                    count(expr("*")).alias("server_total_points_hist"),
                    avg("serve_speed_kmh").alias("server_avg_speed_hist"),
                    Fmax("ts").alias("server_last_seen_ts"),
                )
        )

        receiver_hist = (
            hist.groupBy("receiver")
                .agg(
                    count(expr("*")).alias("receiver_total_points_hist"),
                    avg("rally_length").alias("receiver_avg_rally_hist"),
                    Fmax("ts").alias("receiver_last_seen_ts"),
                )
        )

        enriched = (
            cur.join(server_hist, on="server", how="left")
               .join(receiver_hist, on=cur["receiver"] == receiver_hist["receiver"], how="left")
               .drop(receiver_hist["receiver"])
        )
        return enriched

    def _write_batch_to_kafka(self, df: DataFrame) -> None:
        (
            df.selectExpr("to_json(struct(*)) AS value")
              .write
              .format("kafka")
              .option("kafka.bootstrap.servers", self.kafka_bootstrap)
              .option("topic", self.output_topic)
              .save()
        )

    def _foreach_batch(self, df: DataFrame, epoch_id: int):
        history = self._load_history_snapshot()
        enriched = self._enrich_with_history(df, history)
        self._write_batch_to_kafka(enriched)

    # =========================
    # Query B (enrichment + sink Kafka)
    # =========================
    def _start_enrichment_query(self, df: DataFrame):
        self.q_enr = (
            df.writeStream
              .option("checkpointLocation", self.checkpoint_enr)
              .outputMode("append")
              .foreachBatch(self._foreach_batch)
              .start()
        )
        return self.q_enr

    # =========================
    # Orchestrator
    # =========================
    def run(self):
        events = self._read_stream()
        self._start_historical_query(events)
        self._start_enrichment_query(events)
        self.spark.streams.awaitAnyTermination()


# =========================
# Entrypoint
# =========================
if __name__ == "__main__":
    app = SparkStructuredStreaming()
    app.run()
