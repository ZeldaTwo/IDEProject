package gftt.handler

/** Alert row as written by the alert detector into the PostgreSQL `alerts`
  * table and polled by this component.
  */
final case class Alert(
    id: Long,
    ts: Long,
    device_a: String,
    device_b: String,
    faction_a: String,
    faction_b: String,
    latitude: Double,
    longitude: Double,
    distance_m: Double
)
