package gftt.simulator

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.util.Random

/** Component 1: simulates a fleet of IoT bracelets and pushes their GPS
  * positions to the distributed stream (Kafka topic `positions`).
  */
object Main {

  implicit val positionEncoder: Encoder[Position] = deriveEncoder[Position]

  private val bootstrap  = sys.env.getOrElse("KAFKA_BOOTSTRAP", "localhost:9092")
  private val topic      = sys.env.getOrElse("POSITIONS_TOPIC", "positions")
  private val nbDevices  = sys.env.getOrElse("NB_DEVICES", "40").toInt
  private val intervalMs = sys.env.getOrElse("INTERVAL_MS", "1000").toLong

  // environnmments vals for factions
  private val factions  = Vector("RED", "BLUE")
  private val centerLat = 48.8566 // Paris
  private val centerLon = 2.3522
  private val spread    = 0.01    // 1 km initial spread
  private val jitter    = 0.0004  // 40 m random walk per tick

  private val producerConfig: Map[String, AnyRef] = Map(
    "bootstrap.servers" -> bootstrap,
    "key.serializer"    -> "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer"  -> "org.apache.kafka.common.serialization.StringSerializer",
    "acks"              -> "all"
  )

  private def initialFleet(rng: Random): Map[String, Position] =
    (0 until nbDevices).map { i =>
      val id = f"bracelet-$i%03d"
      id -> Position(
        ts = System.currentTimeMillis(),
        deviceId = id,
        latitude = centerLat + (rng.nextDouble() - 0.5) * spread,
        longitude = centerLon + (rng.nextDouble() - 0.5) * spread,
        faction = factions(i % factions.size),
        status = "ACTIVE"
      )
    }.toMap

  private def step(p: Position, rng: Random): Position =
    p.copy(
      ts = System.currentTimeMillis(),
      latitude = p.latitude + (rng.nextDouble() - 0.5) * jitter,
      longitude = p.longitude + (rng.nextDouble() - 0.5) * jitter,
      status = if (rng.nextDouble() < 0.02) "LOW_BATTERY" else "ACTIVE"
    )

  def main(args: Array[String]): Unit = {
    val rng      = new Random(42)
    val producer = new KafkaProducer[String, String](producerConfig.asJava)
    sys.addShutdownHook(producer.close())

    @tailrec
    def loop(fleet: Map[String, Position], tick: Long): Unit = {
      val moved = fleet.map { case (id, p) => id -> step(p, rng) }
      // key = deviceId
      moved.values.foreach { p =>
        producer.send(new ProducerRecord[String, String](topic, p.deviceId, p.asJson.noSpaces))
      }
      producer.flush()
      println(s"[simulator] tick=$tick sent ${moved.size} positions to '$topic'")
      Thread.sleep(intervalMs)
      loop(moved, tick + 1)
    }

    println(s"[simulator] starting: $nbDevices devices -> $bootstrap topic '$topic'")
    loop(initialFleet(rng), 0L)
  }
}
