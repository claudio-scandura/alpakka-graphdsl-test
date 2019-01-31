val settings = {
  name := "alpakka-graphdsl-test"
  version := "1.0"

  scalaVersion := "2.12.8"
}


val akkaVersion = "2.5.19"

val dependencies = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-contrib" % akkaVersion,

  "org.apache.kafka" %% "kafka" % "2.0.0",

  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",

  "org.scalatest" %% "scalatest" % "3.0.1" % "test,it",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % Test,
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(libraryDependencies ++= dependencies)
  .settings(settings ++ Defaults.itSettings: _*)
