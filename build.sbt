ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val root = (project in file("."))
  .settings(
    name := "delta-standalone"
  )

libraryDependencies ++= Seq(
  "io.delta" %% "delta-standalone" % "0.4.1",
  "org.apache.hadoop" % "hadoop-client" % "3.1.0",
  "org.apache.parquet" % "parquet-hadoop" % "1.10.1",
)

libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.11.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.2.1"
libraryDependencies += "joda-time" % "joda-time" % "2.10.14"
libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.10.1"
libraryDependencies += "org.apache.avro" % "avro" % "1.9.2"
