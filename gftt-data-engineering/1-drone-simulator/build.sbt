name := "drone-simulator"
version := "0.1.0"
scalaVersion := "2.13.12"

val circeVersion = "0.14.6"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients"  % "3.7.1",
  "io.circe"        %% "circe-core"     % circeVersion,
  "io.circe"        %% "circe-generic"  % circeVersion,
  "io.circe"        %% "circe-parser"   % circeVersion,
  "org.slf4j"        % "slf4j-simple"   % "2.0.13"
)

// The producer is a long-running process; fork so Ctrl-C stops cleanly.
fork := true
Compile / run / mainClass := Some("gftt.simulator.Main")
