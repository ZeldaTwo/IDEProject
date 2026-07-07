package gftt.analytics

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Component 5 of the subject (analysis). Reads the curated Silver/Gold zones of
  * the data lake and answers 4 questions with the DataFrame API only. Alerts/combats come from the lake (archived by the weekly job),
  * so this runs fully decoupled from the operational database.
  */
object Main {

  private val lake = sys.env.getOrElse("DATA_LAKE", "hdfs://localhost:9000/gftt/datalake")

  private def exists(spark: SparkSession, path: String): Boolean =
    new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration).exists(new Path(path))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("analytics")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val goldFaction = spark.read.format("parquet").load(s"$lake/gold/positions_per_faction_day")

    println("\n================ ANALYTICS ================\n")

    // Q1 & Q3 need alerts (archived to the lake by the weekly job).
    if (exists(spark, s"$lake/silver/alerts")) {
      val alerts = spark.read.format("parquet").load(s"$lake/silver/alerts")

      // Q1: more alerts on weekdays or weekends?
      println("Q1 - Alerts: weekday vs weekend")
      alerts
        .withColumn("is_weekend", dayofweek(col("event_time")).isin(1, 7))
        .groupBy(col("is_weekend"))
        .agg(count(lit(1)).as("nb_alerts"))
        .orderBy(col("is_weekend"))
        .show(false)

      // Q3: which hour of the day triggers the most fights?
      println("Q3 - Busiest hour for alerts")
      alerts
        .withColumn("hour", hour(col("event_time")))
        .groupBy(col("hour"))
        .agg(count(lit(1)).as("nb_alerts"))
        .orderBy(col("nb_alerts").desc)
        .show(5, false)
    } else println("Q1/Q3 skipped: run 8-weekly-archive then 5-silver-etl first.\n")

    // Q2: which faction is the most active (most GPS positions)?
    println("Q2 - Most active faction (total positions)")
    goldFaction
      .groupBy(col("faction"))
      .agg(sum(col("nb_positions")).as("total_positions"))
      .orderBy(col("total_positions").desc)
      .show(false)

    // Q4: which faction wins the most fights?
    if (exists(spark, s"$lake/gold/faction_ranking")) {
      println("Q4 - Faction with the most victories")
      spark.read.format("parquet").load(s"$lake/gold/faction_ranking").show(false)
    } else println("Q4 skipped: run 8-weekly-archive then 5-silver-etl and 6-gold-etl first.\n")

    println("==========================================\n")
    spark.stop()
  }
}
