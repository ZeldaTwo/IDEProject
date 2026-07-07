package gftt.archive

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/** The "SparkWeekly" component of the architecture: a periodic (weekly) batch
  * job that reads the operational PostgreSQL tables, archives them as Avro into
  * the Bronze zone of the data lake, then purges the tables so the operational
  * DB stays small.
  */
object Main {

  private val lake     = sys.env.getOrElse("DATA_LAKE", "hdfs://localhost:9000/gftt/datalake")
  private val jdbcUrl  = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/gftt")
  private val jdbcUser = sys.env.getOrElse("PG_USER", "gftt")
  private val jdbcPass = sys.env.getOrElse("PG_PASSWORD", "gftt")

  private def jdbc(reader: org.apache.spark.sql.DataFrameReader): org.apache.spark.sql.DataFrameReader =
    reader.option("url", jdbcUrl)
      .option("user", jdbcUser)
      .option("password", jdbcPass)
      .option("driver", "org.postgresql.Driver")

  private def readTable(spark: SparkSession, table: String): DataFrame =
    jdbc(spark.read.format("jdbc")).option("dbtable", table).load()

  private def archive(df: DataFrame, path: String): Unit = {
    df.withColumn("ingest_date", to_date((col("ts") / 1000).cast(TimestampType)))
      .write.mode("append").partitionBy("ingest_date").format("avro").save(path)
    ()
  }

  /** Empty the table while keeping its schema. */
  private def purge(df: DataFrame, table: String): Unit = {
    df.limit(0).write.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", table)
      .option("user", jdbcUser)
      .option("password", jdbcPass)
      .option("driver", "org.postgresql.Driver")
      .option("truncate", "true")
      .mode("overwrite")
      .save()
    ()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("weekly-archive")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val alerts  = readTable(spark, "alerts").persist()
    val combats = readTable(spark, "combat_results").persist()
    val nA = alerts.count()
    val nC = combats.count()

    archive(alerts,  s"$lake/bronze/alerts")
    archive(combats, s"$lake/bronze/combats")

    purge(alerts,  "alerts")
    purge(combats, "combat_results")

    println(s"[weekly-archive] archived $nA alerts and $nC combats to Bronze, then purged PostgreSQL")
    spark.stop()
  }
}
