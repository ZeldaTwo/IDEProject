package gftt.handler

/** Alert message as published on the `alerts` Kafka topic by the detector.
  * Field names match the JSON keys produced by Spark's `to_json`.
  */

final case class Alert(
    ts: Long,
    device_a: String,
    device_b: String,
    faction_a: String,
    faction_b: String,
    latitude: Double,
    longitude: Double,
    distance_m: Double
)
