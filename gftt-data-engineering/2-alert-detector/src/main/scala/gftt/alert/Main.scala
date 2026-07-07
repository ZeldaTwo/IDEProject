package gftt.alert

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Component 2: reads the `positions` stream, detects when two devices of
  * opposite factions are close enough to trigger a fight, and writes the
  * alerts to the PostgreSQL operational DB only (the mobile app reads them
  * straight from there; alerts are later archived to the lake by the weekly
  * job, then read by analytics / dashboard).
  */
object Main {

  private val bootstrap    = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val positions    = sys.env.getOrElse("POSITIONS_TOPIC", "positions")
  private val thresholdM   = sys.env.getOrElse("ALERT_DISTANCE_M", "50").toDouble
  private val lake         = sys.env.getOrElse("DATA_LAKE", "hdfs://localhost:9000/gftt/datalake")

  private val jdbcUrl  = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/gftt")
  private val jdbcUser = sys.env.getOrElse("PG_USER", "gftt")
  private val jdbcPass = sys.env.getOrElse("PG_PASSWORD", "gftt")

  private val positionSchema = StructType(Seq(
    StructField("ts", LongType),
    StructField("deviceId", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("faction", StringType),
    StructField("status", StringType)
  ))

  /** Haversine distance in metres between two lat/lon columns. */
  private def haversine(aLat: String, aLon: String, bLat: String, bLon: String) = {
    val earthR = 6371000.0
    val dLat   = radians(col(bLat) - col(aLat))
    val dLon   = radians(col(bLon) - col(aLon))
    val h = pow(sin(dLat / 2), 2) +
      cos(radians(col(aLat))) * cos(radians(col(bLat))) * pow(sin(dLon / 2), 2)
    lit(2 * earthR) * asin(sqrt(h))
  }

  private def alertsOf(batch: DataFrame): DataFrame = {
    val a = batch.select(
      col("deviceId").as("a_id"), col("faction").as("a_fac"),
      col("latitude").as("a_lat"), col("longitude").as("a_lon"), col("ts").as("a_ts")
    )
    val b = batch.select(
      col("deviceId").as("b_id"), col("faction").as("b_fac"),
      col("latitude").as("b_lat"), col("longitude").as("b_lon"), col("ts").as("b_ts")
    )
    a.join(b, col("a_id") < col("b_id") && col("a_fac") =!= col("b_fac"))
      .withColumn("distance_m", haversine("a_lat", "a_lon", "b_lat", "b_lon"))
      .where(col("distance_m") < lit(thresholdM))
      .select(
        greatest(col("a_ts"), col("b_ts")).as("ts"),
        col("a_id").as("device_a"),
        col("b_id").as("device_b"),
        col("a_fac").as("faction_a"),
        col("b_fac").as("faction_b"),
        ((col("a_lat") + col("b_lat")) / 2).as("latitude"),
        ((col("a_lon") + col("b_lon")) / 2).as("longitude"),
        col("distance_m")
      )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("alert-detector")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val stream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", positions)
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast(StringType), positionSchema).as("d"))
      .select(col("d.*"))

    val query = stream.writeStream
      .option("checkpointLocation", s"$lake/_checkpoints/alert-detector")
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val alerts = alertsOf(batch).persist()
        val n = alerts.count()

        alerts.write.format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", "alerts")
          .option("user", jdbcUser)
          .option("password", jdbcPass)
          .option("driver", "org.postgresql.Driver")
          .mode("append")
          .save()

        println(s"[alert-detector] batch produced $n alert(s)")
        alerts.unpersist()
        ()
      }
      .start()

    query.awaitTermination()
  }
}
