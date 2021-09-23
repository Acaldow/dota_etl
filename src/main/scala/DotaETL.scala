import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scalaj.http._

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


object DotaETL extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("DotaETL")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val request: HttpRequest = Http("https://api.opendota.com/api/players/639740/recentMatches")
  val response = request.asString

  import spark.implicits._
  val df = Seq(response.body).toDS()
  val jsonDF = spark.read.json(df)

  val kdaDF = jsonDF.select(
    $"match_id",
    $"kills",
    $"assists",
    $"deaths",
    ($"kills" + $"assists" / $"deaths").alias("KDA"))
  val nDF = kdaDF.limit(5)

  val myList = nDF.select("match_id").map(f => f.getLong(0)).collectAsList()
  val func = (x: Long) => Http(s"https://api.opendota.com/api/matches/$x").asString.body
  val testList = myList.map(func)
  val matchDS = testList.toList.toDS()
  val matchDF = spark.read.json(matchDS)


  val finalDF = matchDF.select(explode($"players").alias("players"))
  finalDF.show(5, 25, true)
  finalDF.printSchema()

  spark.stop()
  System.exit(0)
}
