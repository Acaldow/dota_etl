name := "SparkersAssignment"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test
)
