package gftt.silver

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** Component 5 (data lake layer 2): batch job that curates the Bronze zone into
  * the Silver zone: drop duplicates, enforce types, keep only valid rows, and
  * repartition by date and faction. Output is Parquet.
  */
object Main {

  private val lake = sys.env.getOrElse("DATA_LAKE", "./datalake")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("silver-etl")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val bronze = spark.read.format("avro").load(s"$lake/bronze/positions")

    val silver = bronze
      .where(col("deviceId").isNotNull && col("ts").isNotNull)
      .dropDuplicates("deviceId", "ts")
      .withColumn("event_time", (col("ts") / 1000).cast(TimestampType))
      .withColumn("date", to_date(col("event_time")))
      .select(
        col("ts"),
        col("event_time"),
        col("date"),
        col("deviceId"),
        col("faction"),
        col("status"),
        col("latitude"),
        col("longitude")
      )

    silver.write
      .mode("overwrite")
      .partitionBy("date", "faction")
      .format("parquet")
      .save(s"$lake/silver/positions")

    println(s"[silver] wrote ${silver.count()} curated rows to $lake/silver/positions")
    spark.stop()
  }
}
