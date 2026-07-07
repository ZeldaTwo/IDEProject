package gftt.handler

import java.sql.{Connection, DriverManager, ResultSet}
import scala.annotation.tailrec
import scala.util.Using

/** Component 3: stands in for the mobile app. PoC simplification: instead of a
  * push channel, it polls the `alerts` table (written by the alert detector)
  * directly for new rows, simulates the push notification, resolves the fight
  * and stores the result in the `combat_results` table.
  */
object Main {

  private val jdbcUrl  = sys.env.getOrElse("PG_URL", "jdbc:postgresql://localhost:5432/gftt")
  private val jdbcUser = sys.env.getOrElse("PG_USER", "gftt")
  private val jdbcPass = sys.env.getOrElse("PG_PASSWORD", "gftt")
  private val pollMs   = sys.env.getOrElse("ALERT_POLL_MS", "2000").toLong

  private val selectSql =
    """SELECT id, ts, device_a, device_b, faction_a, faction_b, latitude, longitude, distance_m
      |FROM alerts WHERE id > ? ORDER BY id ASC""".stripMargin

  private val insertSql =
    """INSERT INTO combat_results
      |(ts, device_a, device_b, winner_faction, loser_faction, latitude, longitude)
      |VALUES (?, ?, ?, ?, ?, ?, ?)""".stripMargin

  /** Deterministic resolution (analysis pertinence does not matter here). */
  private def resolve(a: Alert): (String, String) =
    if (a.ts % 2 == 0) (a.faction_a, a.faction_b) else (a.faction_b, a.faction_a)

  private def toAlert(rs: ResultSet): Alert =
    Alert(
      id = rs.getLong("id"),
      ts = rs.getLong("ts"),
      device_a = rs.getString("device_a"),
      device_b = rs.getString("device_b"),
      faction_a = rs.getString("faction_a"),
      faction_b = rs.getString("faction_b"),
      latitude = rs.getDouble("latitude"),
      longitude = rs.getDouble("longitude"),
      distance_m = rs.getDouble("distance_m")
    )

  @tailrec
  private def readRows(rs: ResultSet, acc: List[Alert]): List[Alert] =
    if (rs.next()) readRows(rs, toAlert(rs) :: acc) else acc.reverse

  private def fetchNewAlerts(conn: Connection, lastId: Long): List[Alert] =
    Using.resource(conn.prepareStatement(selectSql)) { ps =>
      ps.setLong(1, lastId)
      Using.resource(ps.executeQuery())(readRows(_, Nil))
    }

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

  @tailrec
  private def loop(conn: Connection, lastId: Long): Unit = {
    val newAlerts = fetchNewAlerts(conn, lastId)
    newAlerts.foreach(persist(conn, _))
    val nextId = newAlerts.map(_.id).foldLeft(lastId)(_ max _)
    Thread.sleep(pollMs)
    loop(conn, nextId)
  }

  def main(args: Array[String]): Unit = {
    println(s"[handler] polling PostgreSQL 'alerts' table every ${pollMs}ms")
    Using.resource(DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)) { conn =>
      loop(conn, 0L)
    }
  }
}
