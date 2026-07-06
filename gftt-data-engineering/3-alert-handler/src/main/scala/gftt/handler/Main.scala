package gftt.handler

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.KafkaConsumer

import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Component 3: second stream consumer. Reads the `alerts` topic, simulates the
  * mobile push notification, resolves the fight and stores the result in the
  * `combat_results` PostgreSQL table.
  */
  
object Main {

  implicit val alertDecoder: Decoder[Alert] = deriveDecoder[Alert]

  private val bootstrap   = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val alertsTopic = sys.env.getOrElse("ALERTS_TOPIC", "alerts")

  private val jdbcUrl  = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/gftt")
  private val jdbcUser = sys.env.getOrElse("PG_USER", "gftt")
  private val jdbcPass = sys.env.getOrElse("PG_PASSWORD", "gftt")

  private val insertSql =
    """INSERT INTO combat_results
      |(ts, device_a, device_b, winner_faction, loser_faction, latitude, longitude)
      |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin

  private val consumerConfig: Map[String, AnyRef] = Map(
    "bootstrap.servers"  -> bootstrap,
    "group.id"           -> "alert-handler",
    "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "auto.offset.reset"  -> "earliest",
    "enable.auto.commit" -> "false"
  )

  /** Deterministic resolution (analysis pertinence does not matter here). */
  private def resolve(a: Alert): (String, String) =
    if (a.ts % 2 == 0) (a.faction_a, a.faction_b) else (a.faction_b, a.faction_a)

  private def persist(conn: Connection, a: Alert): Unit =
    Using.resource(conn.prepareStatement(insertSql)) { ps =>
      val (winner, loser) = resolve(a)
      ps.setLong(1, a.ts)
      ps.setString(2, a.device_a)
      ps.setString(3, a.device_b)
      ps.setString(4, winner)
      ps.setString(5, loser)
      ps.setDouble(6, a.latitude)
      ps.setDouble(7, a.longitude)
      ps.executeUpdate()
      println(s"[handler] PUSH combat ${a.device_a}(${a.faction_a}) vs " +
        s"${a.device_b}(${a.faction_b}) @${a.distance_m.round}m -> winner $winner")
      ()
    }

  def main(args: Array[String]): Unit = {
    val consumer = new KafkaConsumer[String, String](consumerConfig.asJava)
    consumer.subscribe(java.util.List.of(alertsTopic))
    sys.addShutdownHook(consumer.close())

    @tailrec
    def loop(conn: Connection, handled: Long): Unit = {
      val records = consumer.poll(Duration.ofMillis(500))
      val next = records.asScala.foldLeft(handled) { (acc, rec) =>
        decode[Alert](rec.value()) match {
          case Right(a) => persist(conn, a); acc + 1
          case Left(e)  => println(s"[handler] skipped malformed alert: ${e.getMessage}"); acc
        }
      }
      consumer.commitSync()
      loop(conn, next)
    }

    println(s"[handler] consuming '$alertsTopic' from $bootstrap")
    Using.resource(DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) { conn =>
      loop(conn, 0L)
    }
  }
}
