name := "twitter-stream-analysis"

version := "0.0"

val commonSettings = Seq(
  scalaVersion := "2.11.8"
)

lazy val `setup` = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(libraryDependencies ++= Dependencies.projectDependencies)