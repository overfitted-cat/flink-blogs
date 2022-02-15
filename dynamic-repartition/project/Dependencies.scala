import sbt._

object Dependencies {
  lazy val scalaLoggingVersion = "3.9.2"
  lazy val jodaConvertVersion = "2.2.1"
  lazy val flinkVersion = "1.14.2"
  lazy val logbackVersion = "1.2.10"
  lazy val jodaTimeVersion = "2.10.13"

  val commons = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "ch.qos.logback" % "logback-classic" % logbackVersion,
    "org.joda" % "joda-convert" % jodaConvertVersion,
    "joda-time" % "joda-time" % jodaTimeVersion
  )

  val prod = Seq(
    "org.apache.flink" %% "flink-scala" % flinkVersion % Provided,
    "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Provided,
    "org.apache.flink" %% "flink-clients" % flinkVersion % Provided
  )

}