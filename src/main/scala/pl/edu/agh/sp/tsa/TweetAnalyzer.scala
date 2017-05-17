package pl.edu.agh.sp.tsa

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
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

/**
  * An object responsible for establishing connection with Twitter Streaming API and proceed real-time tweets analysis.
  * To do that it invoke previously created Naive Bayes Model.
  * At the end of the iteration data are publish to Redis channel called 'TweetStream'.
  *
  * @author Mateusz Kluska
  */
object TweetAnalyzer {
  /** Starts TweetAnalyzer
    *
    * @param args not used
    */
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
    ssc.awaitTerminationOrTimeout(60 * 1000 * 15)
  }

  /** Creates Streaming Context establish on Twitter Streaming.
    * Here could be configure spark cluster, we set local processor with four cores, but it could much more efficient machine
    *
    * @return created StreamingContext
    */
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

  /** Load authorization data from app.conf file and create authorization object with them
    *
    * @return Some of OAuthAuthorization object filled with data from configuration
    */
  def getTwitterAuth: Some[OAuthAuthorization] = {
    val conf = new ConfigurationBuilder()
    conf.setOAuthConsumerKey(ConfigLoader.consumerKey)
    conf.setOAuthConsumerSecret(ConfigLoader.consumerSecret)
    conf.setOAuthAccessToken(ConfigLoader.accessToken)
    conf.setOAuthAccessTokenSecret(ConfigLoader.accessTokenSecret)
    val oAuth = Some(new OAuthAuthorization(conf.build()))
    oAuth
  }

  /** Invokes method to computeSentiment, takes geoLocation from Status and return light properly filled TweetWithSentiment object.
    *
    * @param status    tweet status, which sentiment will be calculate
    * @param model     Naive Bayes Model, which will be used to predict tweet sentiment
    * @param stopWords shared between threads list of english stop words, taken from NLTK
    * @return light TweetWithSentiment Object with calculated data
    */
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

  /**
    * Check if tweet has english content. This is important, because used model can compute only english tweets sentiment.
    *
    * @param status tweet Status which language type will be checked
    * @return true if tweet has english content, false in the other case
    */
  def isTweetInEnglish(status: Status): Boolean = {
    status.getUser.getScreenName
    status.getLang == "en" && status.getUser.getLang == "en"
  }

  /**
    * Check if tweet has geolocation tag or approximate place info
    *
    * @param status tweet Status, which will be checked
    * @return true if tweet has geolocation or place info
    */
  def hasGeoLocationOrPlace(status: Status): Boolean = {
    status.getGeoLocation != null || status.getPlace != null
  }


  /**
    * Calculate approximate tweet location based on tweet place bounding context coordinates.
    *
    * @param place tweet origin place which from bounding context coo are taken
    * @return GeoLocation object with calculated latitude and longitude
    */
  def getCoordinatesFromTweetBoundingBox(place: Place): GeoLocation = {
    val leftBottom = place.getBoundingBoxCoordinates.apply(0).apply(0)
    val rightBottom = place.getBoundingBoxCoordinates.apply(0).apply(1)
    val leftTop = place.getBoundingBoxCoordinates.apply(0).apply(3)
    val long = (leftBottom.getLongitude + leftTop.getLongitude) / 2
    val lat = (leftBottom.getLatitude + rightBottom.getLatitude) / 2

    new GeoLocation(lat, long)
  }


  /**
    * Publish computed data to Redis channel 'TweetStream' as json strings, due to easy frontend compatibility
    *
    * @param analysedTweets Data Stream with previously analysed tweets
    */
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