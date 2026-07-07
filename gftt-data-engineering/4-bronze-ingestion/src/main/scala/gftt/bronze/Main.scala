package gftt.bronze

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Component 4 (data lake layer 1): consumes the `positions` stream and stores
  * the raw messages, untouched, in the Bronze zone of the data lake.
  */
object Main {

  private val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val topic     = sys.env.getOrElse("POSITIONS_TOPIC", "positions")
  private val lake      = sys.env.getOrElse("DATA_LAKE", "./datalake")

  private val positionSchema = StructType(Seq(
    StructField("ts", LongType),
    StructField("deviceId", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("faction", StringType),
    StructField("status", StringType)
  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bronze-ingestion")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val parsed = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast(StringType), positionSchema).as("d"))
      .select(col("d.*"))
      .withColumn("event_time", (col("ts") / 1000).cast(TimestampType))
      .withColumn("date", to_date(col("event_time")))

    val query = parsed.writeStream
      .format("avro")
      .option("path", s"$lake/bronze/positions")
      .option("checkpointLocation", s"$lake/_checkpoints/bronze")
      .partitionBy("date")
      .outputMode("append")
      .start()

    println(s"[bronze] writing raw Avro to $lake/bronze/positions")
    query.awaitTermination()
  }
}
