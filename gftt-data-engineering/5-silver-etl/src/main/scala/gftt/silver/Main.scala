package gftt.silver

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Component 5 (data lake layer 2): batch job that curates the Bronze zone into
  * the Silver zone. Positions are parsed here (schema-on-read from the raw
  * Bronze payload); combats and alerts archived from PostgreSQL by the weekly
  * job are cleaned. Everything is deduplicated, typed, partitioned by date
  * (positions also by faction) and written as Parquet (columnar, query-ready).
  */
object Main {

  private val lake = sys.env.getOrElse("DATA_LAKE", "hdfs://localhost:9000/gftt/datalake")

  private val positionSchema = StructType(Seq(
    StructField("ts", LongType),
    StructField("deviceId", StringType),
    StructField("latitude", DoubleType),
    StructField("longitude", DoubleType),
    StructField("faction", StringType),
    StructField("status", StringType)
  ))

  private def exists(spark: SparkSession, path: String): Boolean =
    new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration).exists(new Path(path))

  private def withDate(df: DataFrame): DataFrame =
    df.withColumn("event_time", (col("ts") / 1000).cast(TimestampType))
      .withColumn("date", to_date(col("event_time")))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("silver-etl")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // 1) positions: parse the raw Bronze payload, then curate.
    val positions = withDate(
      spark.read.format("avro").load(s"$lake/bronze/positions")
        .select(from_json(col("value"), positionSchema).as("d"))
        .select(col("d.*"))
        .where(col("deviceId").isNotNull && col("ts").isNotNull)
        .dropDuplicates("deviceId", "ts")
    ).select("ts", "event_time", "date", "deviceId", "faction", "status", "latitude", "longitude")

    positions.write.mode("overwrite")
      .partitionBy("date", "faction").format("parquet")
      .save(s"$lake/silver/positions")
    println(s"[silver] wrote ${positions.count()} curated positions")

    // 2) combats archived from PostgreSQL (present only after the weekly job ran).
    if (exists(spark, s"$lake/bronze/combats")) {
      val combats = withDate(
        spark.read.format("avro").load(s"$lake/bronze/combats")
          .dropDuplicates("ts", "device_a", "device_b")
      )
      combats.write.mode("overwrite")
        .partitionBy("date").format("parquet")
        .save(s"$lake/silver/combats")
      println(s"[silver] wrote ${combats.count()} curated combats")
    }

    // 3) alerts archived from PostgreSQL (present only after the weekly job ran).
    if (exists(spark, s"$lake/bronze/alerts")) {
      val alerts = withDate(
        spark.read.format("avro").load(s"$lake/bronze/alerts")
          .dropDuplicates("ts", "device_a", "device_b")
      )
      alerts.write.mode("overwrite")
        .partitionBy("date").format("parquet")
        .save(s"$lake/silver/alerts")
      println(s"[silver] wrote ${alerts.count()} curated alerts")
    }

    spark.stop()
  }
}
