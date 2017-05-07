package pl.edu.agh.sp.tsa

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import pl.edu.agh.sp.tsa.model.Tweet
import spark._

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
    Spark.after(new Filter {
      override def handle(request: Request, response: Response): Unit = {
        response.header("Access-Control-Allow-Origin", "*")
      }
    })

    Spark.get("/tweets", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        val hashtags: Set[String] = Option(request.queryParamsValues("hashtags[]"))
          .getOrElse(Array[String]())
          .toSet

        val numberOfTweets: Int = Option(request.queryParams("numberOfTweets"))
          .getOrElse("15")
          .toInt

        val tweets: List[JSONObject] = getTopTweets(hashtags, numberOfTweets).map(_.toJSONObject)
        JSONArray.apply(tweets).toString(JSONFormat.defaultFormatter)
      }
    })

    Spark.get("/hashtags", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        val numberOfHashtags: Int = Option(request.queryParams("numberOfHashtags"))
          .getOrElse("15")
          .toInt

        val hashtagsMap: Map[String, Long] = getTopHashtags(numberOfHashtags)
        JSONObject.apply(hashtagsMap).toString(JSONFormat.defaultFormatter)
      }
    })
  }

  private def getTopHashtags(numberOfHashtags: Int): Map[String, Long] = {
    dataFrame
      .select("entities.hashtags.text")
      .withColumn("text", explode(col("text")))
      .groupBy("text")
      .count()
      .orderBy(desc("count"))
      .limit(numberOfHashtags)
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap[String, Long]
  }

  private def getTopTweets(hashtags: Set[String], numberOfTweets: Int): List[Tweet] = {
    dataFrame
      .select(
        "text",
        "quoted_status.retweet_count",
        "quoted_status.favorite_count",
        "entities.hashtags.text"
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
      .getAs[mutable.WrappedArray[String]](3)
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
    new Tweet(text, retweetCount, favoriteCount, hashtags)
  }
}
