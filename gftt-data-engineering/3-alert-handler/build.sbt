name := "alert-handler"
version := "0.1.0"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.7.3"
)

fork := true
Compile / run / mainClass := Some("gftt.handler.Main")
