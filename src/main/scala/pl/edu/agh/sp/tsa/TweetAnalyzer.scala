package pl.edu.agh.sp.tsa

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Durations, StreamingContext}
import pl.edu.agh.sp.tsa.machine_learning.{MLibSentimentAnalyzer, SparkNaiveBayesModelCreator}
import pl.edu.agh.sp.tsa.model.TweetWithSentiment
import pl.edu.agh.sp.tsa.util.ConfigLoader
import redis.clients.jedis.Jedis
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.{GeoLocation, Place, Status}

import scala.util.parsing.json.JSONFormat

object TweetAnalyzer {
  def hashingTF = new HashingTF()

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createStreamingContext)
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, ConfigLoader.naiveBayesModelPath)
    val stopWords = ssc.sparkContext.broadcast(SparkNaiveBayesModelCreator.loadStopWordsFromFile())
    val tweets = TwitterUtils.createStream(ssc, getTwitterAuth)

    val analysedTweets = tweets
      .filter(hasGeoLocationOrPlace)
      .map(status => predictSentiment(status, naiveBayesModel, stopWords))

    publishToRedis(analysedTweets)

    ssc.start()
    ssc.awaitTerminationOrTimeout(60 * 1000 * 5)
  }

  def createStreamingContext(): StreamingContext = {
    val conf = new SparkConf()
      .setAppName("Twitter Stream Analysis")
      .setMaster("local[4]")
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

  def predictSentiment(status: Status, model: NaiveBayesModel, stopWords: Broadcast[List[String]]): TweetWithSentiment = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val normalizedSentiment = MLibSentimentAnalyzer.computeSentiment(status, model, stopWords)
    val geoLoc = Option(status.getGeoLocation).getOrElse(getCoordinatesFromTweetBoundingBox(status.getPlace))
    new TweetWithSentiment(status.getId,
      status.getUser.getScreenName,
      status.getText,
      normalizedSentiment,
      geoLoc.getLatitude,
      geoLoc.getLongitude,
      status.getUser.getOriginalProfileImageURL,
      format.format(status.getCreatedAt))
  }

  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
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

  def publishToRedis(analysedTweets: DStream[TweetWithSentiment]): Unit = {
    val redisHost = ConfigLoader.redisHost
    val redisPort = ConfigLoader.redisPort
    analysedTweets.foreachRDD { rdd =>
      if (rdd != null && !rdd.isEmpty() && !rdd.partitions.isEmpty) {
        rdd.foreach {
          tweet =>
            val jedis = new Jedis(redisHost, redisPort)
            val writable = tweet.toJSONObject.toString(JSONFormat.defaultFormatter)
            val pipeline = jedis.pipelined()
            pipeline.publish("TweetStream", writable)
            pipeline.sync()
        }
      }
    }
  }
}