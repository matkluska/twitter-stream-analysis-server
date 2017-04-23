package pl.edu.agh.sp.tsa

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import pl.edu.agh.sp.tsa.util.ConfigLoader
import redis.clients.jedis.Jedis
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object TweetAnalyzer {
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)
    val tweets = TwitterUtils.createStream(ssc, getTwitterAuth)

    val analysedTweets = tweets.filter(hasGeoLocation)
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
//    System.setProperty("twitter4j.oauth.consumerKey", "xIX9eIFYJSkpIUS6jUzpuA6Ah")
//    System.setProperty("twitter4j.oauth.consumerSecret", "xcNjwLLz4tZ7a6wY5U4x6FMsltGb2ziZ8hn2dB9sKxKizss5xO")
//    System.setProperty("twitter4j.oauth.accessToken", "853564756808146944-gpiupUsq8xj6xwwXKjwC0ABEvt9zHij")
//    System.setProperty("twitter4j.oauth.accessTokenSecret", "7dShyiotO0YMhOYEW9cbzSzS7JQ1mkjmRWiXCaHu2z7lY")

    val conf = new ConfigurationBuilder()
        conf.setOAuthConsumerKey(ConfigLoader.consumerKey)
        conf.setOAuthConsumerSecret(ConfigLoader.consumerSecret)
        conf.setOAuthAccessToken(ConfigLoader.accessToken)
        conf.setOAuthAccessTokenSecret(ConfigLoader.accessTokenSecret)
    val oAuth = Some(new OAuthAuthorization(conf.build()))
    oAuth
  }


  def predictSentiment(status: Status): (Long, String, String, Int, Double, Double, String, String) = {
    (status.getId,
      status.getUser.getScreenName,
      status.getText,
      0,
      status.getGeoLocation.getLatitude,
      status.getGeoLocation.getLongitude,
      status.getUser.getOriginalProfileImageURL,
      status.getCreatedAt.formatted("EE MMM dd HH:mm:ss ZZ yyyy"))
  }

  def hasGeoLocation(status: Status): Boolean = {
    status.getGeoLocation != null
  }
}