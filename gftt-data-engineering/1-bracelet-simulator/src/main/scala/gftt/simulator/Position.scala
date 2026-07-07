package gftt.simulator

/** One GPS message emitted by an IoT bracelet.
  * Fields: timestamp, device id, latitude, longitude,
  * plus two useful fields for the game: faction and status.
  */
final case class Position(
    ts: Long,
    deviceId: String,
    latitude: Double,
    longitude: Double,
    faction: String,
    status: String
)
