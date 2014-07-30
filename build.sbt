
organization := "com.gilt"

name := "escalator"

version := "1.0.0-SNAPSHOT"

crossScalaVersions := Seq("2.10.4", "2.11.1")

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.1.0",
  "com.typesafe.play" %% "play-iteratees" % "2.2.3",
  "org.scalatest" %% "scalatest" % "2.2.0" % "test"
)

instrumentSettings

ScoverageKeys.highlighting := true

// kinesis streams are precious, don't run in parallel!
parallelExecution in ScoverageTest := false

parallelExecution in Test := false


