package gftt.simulator

/** One GPS message emitted by an IoT bracelet.
  * Fields required by the subject: timestamp, device id, latitude, longitude,
  * plus the two useful fields for the game: faction and status.
  */
final case class Position(
    ts: Long,
    deviceId: String,
    latitude: Double,
    longitude: Double,
    faction: String,
    status: String
)
