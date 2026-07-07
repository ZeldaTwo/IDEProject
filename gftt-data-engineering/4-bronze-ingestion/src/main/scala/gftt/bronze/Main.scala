package gftt.bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/** Component 4 (data lake layer 1): consumes the `positions` stream and stores
  * the raw Kafka messages, untouched, in the Bronze zone of the data lake.
  */
object Main {

  private val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val topic     = sys.env.getOrElse("POSITIONS_TOPIC", "positions")
  private val lake      = sys.env.getOrElse("DATA_LAKE", "hdfs://localhost:9000/gftt/datalake")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bronze-ingestion")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(
        col("key").cast(StringType).as("key"),
        col("value").cast(StringType).as("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").as("kafka_ts")
      )
      .withColumn("ingest_date", to_date(col("kafka_ts")))

    val query = raw.writeStream
      .format("avro")
      .option("path", s"$lake/bronze/positions")
      .option("checkpointLocation", s"$lake/_checkpoints/bronze")
      .partitionBy("ingest_date")
      .outputMode("append")
      .start()

    println(s"[bronze] writing raw Avro to $lake/bronze/positions")
    query.awaitTermination()
  }
}
