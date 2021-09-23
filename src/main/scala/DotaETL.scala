import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scalaj.http._

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`


object DotaETL extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("DotaETL")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val account_id = 639740
  val request: HttpRequest = Http(s"https://api.opendota.com/api/players/$account_id/recentMatches")
  val response = request.asString

  import spark.implicits._
  val df = Seq(response.body).toDS()
  val jsonDF = spark.read.json(df)

  val kdaDF = jsonDF.select(
    lit(account_id).alias("account_id"),
    $"match_id",
    $"kills",
    $"assists",
    $"deaths",
    ($"kills" + $"assists" / $"deaths").alias("KDA"))
  val accountDF = kdaDF.limit(5)

  val request2: HttpRequest = Http(s"https://api.opendota.com/api/players/$account_id")
  val response2 = request2.asString
  val playerDS = Seq(response2.body).toDS()
  val playerDF = spark.read.json(playerDS).select("profile.account_id", "profile.name")

  val finalDF = accountDF.join(playerDF, "account_id")

  val myList = finalDF.select("match_id").map(f => f.getLong(0)).collectAsList()
  val func = (x: Long) => Http(s"https://api.opendota.com/api/matches/$x").asString.body
  val matchList = myList.map(func)
  val matchDS = matchList.toList.toDS()
  val matchDF = spark.read.json(matchDS)

  val w = Window.partitionBy("players.isRadiant")
  val parsedDF = matchDF.select(explode($"players").alias("players")).select(
    $"players.account_id",
    $"players.isRadiant",
    $"players.kills",
    $"players.assists",
    sum("players.kills").over(w).alias("Total Kills"))
  parsedDF.show(10, false)

  spark.stop()
  System.exit(0)
}
