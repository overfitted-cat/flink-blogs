lazy val commons = Seq(
  name := "dynamic-reparition",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.12.1"
)

lazy val feature_extraction = project.in(file("."))
  .settings(commons: _*)
  .settings(
    libraryDependencies ++= Dependencies.commons,
    libraryDependencies ++= Dependencies.prod,
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
