name := "SparkersAssignment"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "com.lihaoyi" %% "upickle" % "1.4.1",
  "com.lihaoyi" %% "os-lib" % "0.7.8",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)
