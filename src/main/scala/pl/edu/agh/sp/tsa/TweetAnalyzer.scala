package pl.edu.agh.sp.tsa

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import pl.edu.agh.sp.tsa.util.ConfigLoader
import redis.clients.jedis.Jedis
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{GeoLocation, Place, Status}

object TweetAnalyzer {
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)
    val tweets = TwitterUtils.createStream(ssc, getTwitterAuth)

    val analysedTweets = tweets
      .filter(hasGeoLocationOrPlace)
      .map(predictSentiment)

    analysedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        rdd.foreach {
          case (id, screenName, text, sentiment, latitude, longitude, profileURL, date) =>
            val tuple = (id, screenName, text, sentiment, latitude, longitude, profileURL, date)
            val jedis = new Jedis("localhost", 6379)
            val write = tuple.productIterator.mkString("~|~")
            val pipeline = jedis.pipelined()
            pipeline.publish("TweetStream", write)
            pipeline.sync()
        }
      }
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(60 * 1000 * 5)
  }

  def createStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName("Twitter Stream Analysis")
      .setMaster("local[2]")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.eventLog.enabled", "true")
      .set("spark.streaming.unpersist", "true")

    val ssc = new StreamingContext(conf, Durations.seconds(1))
    ssc
  }

  def getTwitterAuth: Some[OAuthAuthorization] = {
    val conf = new ConfigurationBuilder()
    conf.setOAuthConsumerKey(ConfigLoader.consumerKey)
    conf.setOAuthConsumerSecret(ConfigLoader.consumerSecret)
    conf.setOAuthAccessToken(ConfigLoader.accessToken)
    conf.setOAuthAccessTokenSecret(ConfigLoader.accessTokenSecret)
    val oAuth = Some(new OAuthAuthorization(conf.build()))
    oAuth
  }


  def predictSentiment(status: Status): (Long, String, String, Int, Double, Double, String, String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val geoLoc = Option(status.getGeoLocation).getOrElse(getCoordinatesFromTweetBoundingBox(status.getPlace))
    (status.getId,
      status.getUser.getScreenName,
      status.getText,
      0,
      geoLoc.getLatitude,
      geoLoc.getLongitude,
      status.getUser.getOriginalProfileImageURL,
      format.format(status.getCreatedAt))
  }

  def hasGeoLocationOrPlace(status: Status): Boolean = {
    status.getGeoLocation != null || status.getPlace != null
  }

  def getCoordinatesFromTweetBoundingBox(place: Place): GeoLocation = {
    val leftBottom = place.getBoundingBoxCoordinates.apply(0).apply(0)
    val rightBottom = place.getBoundingBoxCoordinates.apply(0).apply(1)
    val leftTop = place.getBoundingBoxCoordinates.apply(0).apply(3)
    val long = (leftBottom.getLongitude + leftTop.getLongitude) / 2
    val lat = (leftBottom.getLatitude + rightBottom.getLatitude) / 2

    new GeoLocation(lat, long)
  }
}