package pl.edu.agh.sp.tsa

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import pl.edu.agh.sp.tsa.model.Tweet
import pl.edu.agh.sp.tsa.util.ConfigLoader
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
    .json(ConfigLoader.hdfsHost + ConfigLoader.hdfsTweetsPath)

  private val dtf = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")

  def main(args: Array[String]): Unit = {
    Spark.after(new Filter {
      override def handle(request: Request, response: Response): Unit = {
        response.header("Access-Control-Allow-Origin", "*")
        response.`type`("application/json")
      }
    })

    Spark.get("/tweets", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        val hashtags: Set[String] = Option(request.queryParamsValues("hashtags[]"))
          .getOrElse(Array[String]())
          .toSet

        val numberOfTweets: Int = Option(request.queryParams("numberOfTweets"))
          .getOrElse("10")
          .toInt

        val period: Int = Option(request.queryParams("period"))
          .getOrElse("0")
          .toInt

        val tweets: List[JSONObject] = getTopTweets(hashtags, numberOfTweets, period).map(_.toJSONObject)
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

  private def getTopTweets(hashtags: Set[String], numberOfTweets: Int, period: Int): List[Tweet] = {
    dataFrame
      .select(
        "user.screen_name",
        "id_str",
        "entities.hashtags.text",
        "created_at"
      )
      .filter(row => period == 0 || matchPeriod(row, period))
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

  private def matchPeriod(row: Row, period: Int): Boolean = {
    DateTime
      .parse(row.getString(3), dtf)
      .isAfter(
        DateTime
          .now()
          .minusMinutes(period)
          .getMillis
      )
  }

  private def mapRowToTweet(row: Row): Tweet = {
    val username: String = row.getString(0)
    val id: String = row.getString(1)
    new Tweet(id, username)
  }
}