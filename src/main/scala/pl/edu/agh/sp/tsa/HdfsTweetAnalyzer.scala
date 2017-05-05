package pl.edu.agh.sp.tsa

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
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
        val hashtags: List[String] = Option(request.queryParamsValues("hashtags"))
          .getOrElse(Array[String]())
          .toList

        val numberOfTweets: Int = Option(request.queryParams("numberOfTweets"))
          .getOrElse("10")
          .toInt

        response.header("Access-Control-Allow-Origin", "*")

        val tweets: List[JSONObject] = getTopTweets(hashtags, numberOfTweets).map(_.toJSONObject)
        JSONArray.apply(tweets).toString(JSONFormat.defaultFormatter)
      }
    })
  }

  def getTopTweets(hashtags: List[String], numberOfTweets: Int): List[Tweet] = {
    dataFrame
      .select(
        "text",
        "quoted_status.retweet_count",
        "entities.hashtags"
      )
      //.filter() TODO filter against given hashtags
      .orderBy(desc("retweet_count"))
      .limit(numberOfTweets)
      .collect()
      .map(row => {
        val text = row.getString(0)
        val retweetCount = row.getLong(1)
        val hashtags = row.getAs[mutable.WrappedArray[String]](2).toList
        new Tweet(text, retweetCount, hashtags)
      })
      .toList
  }
}
