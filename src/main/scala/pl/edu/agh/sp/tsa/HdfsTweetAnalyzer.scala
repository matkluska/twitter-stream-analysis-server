package pl.edu.agh.sp.tsa

import org.apache.spark.sql.SparkSession
import pl.edu.agh.sp.tsa.model.Tweet
import spark.{Request, Response, Route, Spark}

import scala.util.parsing.json.{JSONArray, JSONFormat, JSONObject}

object HdfsTweetAnalyzer {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    Spark.get("/", new Route {
      override def handle(request: Request, response: Response): AnyRef = {
        val hashtags: List[String] = Option(request.queryParamsValues("hashtags"))
          .getOrElse(Array[String]())
          .toList

        response.header("Access-Control-Allow-Origin", "*")

        val tweets: List[JSONObject] = getTopTenTweets(hashtags).map(_.toJSONObject)
        JSONArray.apply(tweets).toString(JSONFormat.defaultFormatter)
      }
    })

    sparkSession
      .sqlContext
      .read
      .json("hdfs://0.0.0.0:9000/user/flume/tweets")
      .createOrReplaceTempView("tweets")
  }

  def getTopTenTweets(hashtags: List[String]): List[Tweet] = {
    println(hashtags)

    sparkSession
      .sqlContext
      .sql("" +
        "SELECT text " +
        "FROM tweets " +
        "ORDER BY quoted_status.retweet_count DESC " +
        "LIMIT 10"
      )
      .collect()
      .map(row => {
        val text = row.getString(0)
        new Tweet(text)
      })
      .toList
  }
}
