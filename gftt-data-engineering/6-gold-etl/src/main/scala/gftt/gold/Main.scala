package gftt.gold

import java.nio.file.Files
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Component 6 (data lake layer 3): batch job that builds the Gold zone, i.e.
  * business-ready aggregates for the dashboard: per-faction stats, faction
  * ranking and combat history. Pure DataFrame API (no Spark SQL strings). It
  * also exports the (small) tables as a `data.js` file consumed by the static
  * dashboard.
  */
object Main {

  private val lake     = sys.env.getOrElse("DATA_LAKE", "hdfs://localhost:9000/gftt/datalake")
  private val dashData = sys.env.getOrElse("DASHBOARD_DATA", "../dashboard/data.js")

  private def exists(spark: SparkSession, path: String): Boolean =
    new Path(path).getFileSystem(spark.sparkContext.hadoopConfiguration).exists(new Path(path))

  /** Serialise a small aggregate DataFrame to a JSON array (driver-side). */
  private def rowsToJson(df: DataFrame): String = {
    val cols = df.columns.toList
    df.collect().toList.map { r =>
      cols.zipWithIndex.map { case (c, i) =>
        // Gold aggregates never contain empty cells; 0 is a defensive fallback.
        val cell = Option(r(i)) match {
          case Some(s: String) => "\"" + s + "\""
          case Some(other)     => other.toString
          case None            => "0"
        }
        "\"" + c + "\":" + cell
      }.mkString("{", ",", "}")
    }.mkString("[", ",", "]")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("gold-etl")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val silver = spark.read.format("parquet").load(s"$lake/silver/positions")
      .withColumn("hour", hour(col("event_time")))
      .withColumn("dow", dayofweek(col("event_time")))
      .withColumn("is_weekend", col("dow").isin(1, 7))

    // Gold table 1: number of positions per faction per day.
    val positionsPerFactionDay = silver
      .groupBy(col("date"), col("faction"))
      .agg(count(lit(1)).as("nb_positions"))
    positionsPerFactionDay.write.mode("overwrite").format("parquet")
      .save(s"$lake/gold/positions_per_faction_day")

    // Gold table 2: distinct active devices per hour of day.
    val activityPerHour = silver
      .groupBy(col("hour"))
      .agg(countDistinct(col("deviceId")).as("active_devices"))
      .orderBy(col("hour"))
    activityPerHour.write.mode("overwrite").format("parquet")
      .save(s"$lake/gold/activity_per_hour")

    // Gold table 3: weekday vs weekend movement volume.
    val volumeByDayType = silver
      .groupBy(col("is_weekend"))
      .agg(count(lit(1)).as("nb_positions"))
    volumeByDayType.write.mode("overwrite").format("parquet")
      .save(s"$lake/gold/volume_by_day_type")

    // Gold table 4 & 5: faction ranking + combat history (only if combats were
    // archived to Silver by the weekly job).
    val hasCombats = exists(spark, s"$lake/silver/combats")
    val factionRanking =
      if (hasCombats) {
        val combats = spark.read.format("parquet").load(s"$lake/silver/combats")

        val ranking = combats
          .groupBy(col("winner_faction"))
          .agg(count(lit(1)).as("nb_wins"))
          .orderBy(col("nb_wins").desc)
        ranking.write.mode("overwrite").format("parquet")
          .save(s"$lake/gold/faction_ranking")

        val history = combats
          .groupBy(col("date"))
          .agg(count(lit(1)).as("nb_combats"))
          .orderBy(col("date"))
        history.write.mode("overwrite").format("parquet")
          .save(s"$lake/gold/combat_history")

        ranking
      } else spark.emptyDataFrame

    // Export a tiny data.js for the static dashboard.
    val perFactionTotal = positionsPerFactionDay
      .groupBy(col("faction")).agg(sum(col("nb_positions")).as("total_positions"))
      .orderBy(col("total_positions").desc)
    val rankingJson = if (hasCombats) rowsToJson(factionRanking) else "[]"
    val js =
      "window.GOLD = {" +
        "\"positions_per_faction\":" + rowsToJson(perFactionTotal) + "," +
        "\"activity_per_hour\":"     + rowsToJson(activityPerHour) + "," +
        "\"volume_by_day_type\":"    + rowsToJson(volumeByDayType) + "," +
        "\"faction_ranking\":"       + rankingJson +
      "};\n"
    Files.write(java.nio.file.Path.of(dashData), js.getBytes("UTF-8"))

    println(s"[gold] wrote gold tables under $lake/gold/ and dashboard data to $dashData")
    spark.stop()
  }
}
