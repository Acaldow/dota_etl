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
  val nameDS = Seq(response2.body).toDS()
  val nameDF = spark.read.json(nameDS).select("profile.account_id", "profile.name")

  val playerDF = accountDF.join(nameDF, "account_id")

  val myList = playerDF.select("match_id").map(f => f.getLong(0)).collectAsList()
  val func = (x: Long) => Http(s"https://api.opendota.com/api/matches/$x").asString.body
  val matchList = myList.map(func)
  val matchDS = matchList.toList.toDS()
  val matchDF = spark.read.json(matchDS)

  val w = Window.partitionBy("match_id", "players.isRadiant")
  val parsedDF = matchDF.select($"match_id", explode($"players").alias("players")).select(
    $"match_id",
    $"players.account_id",
    $"players.isRadiant",
    $"players.kills",
    $"players.assists",
    sum("players.kills").over(w).alias("total_team_kills"))
  val kpDF = parsedDF.select(
    $"match_id",
    $"account_id",
    when($"isRadiant" === true, "Radiant").otherwise("Dire").alias("team"),
    ((($"kills" + $"assists")/$"total_team_kills")*100).alias("KP"))

  val summaryDF = playerDF.join(kpDF, Seq("account_id", "match_id"))
  val aggregatedDF = summaryDF.groupBy("name")
    .agg(
      count("match_id").as("total_games"),
      max("KDA").as("max_kda"),
      min("KDA").as("min_kda"),
      avg("KDA").as("avg_kda"),
      max("KP").as("max_kp"),
      min("KP").as("min_kp"),
      avg("KP").as("avg_kp"))
  aggregatedDF.show(false)

  spark.stop()
  System.exit(0)
}
