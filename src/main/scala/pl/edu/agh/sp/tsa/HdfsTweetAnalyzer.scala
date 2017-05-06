package pl.edu.agh.sp.tsa

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import pl.edu.agh.sp.tsa.model.Tweet
import spark.{Request, Response, Route, Spark}

import scala.collection.mutable
import scala.util.parsing.json.{JSONArray, JSONFormat, JSONObject}

object HdfsTweetAnalyzer {

  private val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  private val dataFrame: DataFrame = sparkSession
    .sqlContext
    .read
    .json("hdfs://0.0.0.0:9000/user/flume/tweets")

  def main(args: Array[String]): Unit = {
    Spark.get("/", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        val hashtags: Set[String] = Option(request.queryParamsValues("hashtags"))
          .getOrElse(Array[String]())
          .toSet

        val numberOfTweets: Int = Option(request.queryParams("numberOfTweets"))
          .getOrElse("10")
          .toInt

        response.header("Access-Control-Allow-Origin", "*")

        val tweets: List[JSONObject] = getTopTweets(hashtags, numberOfTweets).map(_.toJSONObject)
        JSONArray.apply(tweets).toString(JSONFormat.defaultFormatter)
      }
    })
  }

  private def getTopTweets(hashtags: Set[String], numberOfTweets: Int): List[Tweet] = {
    dataFrame
      .select(
        "text",
        "quoted_status.retweet_count",
        "quoted_status.favorite_count",
        "entities.hashtags.text",
        "quoted_status.user.name"
      )
      .filter(row => hashtags.isEmpty || areCommonHashtags(row, hashtags))
      .orderBy(desc("retweet_count"))
      .limit(numberOfTweets)
      .collect()
      .map(mapRowToTweet)
      .toList
  }

  private def areCommonHashtags(row: Row, hashtags: Set[String]): Boolean = {
    row
      .getAs[mutable.WrappedArray[String]](2)
      .toSet
      .intersect(hashtags)
      .nonEmpty
  }

  private def mapRowToTweet(row: Row): Tweet = {
    val text: String = row.getString(0)
    val retweetCount: Long = Option(row.get(1)) match {
      case Some(value) => value.asInstanceOf[Long]
      case None => 0L
    }
    val favoriteCount: Long = Option(row.get(2)) match {
      case Some(value) => value.asInstanceOf[Long]
      case None => 0L
    }
    val hashtags: List[String] = row.getAs[mutable.WrappedArray[String]](3).toList
    val username: String = row.getString(4)
    new Tweet(text, retweetCount, favoriteCount, hashtags, username)
  }
}
