package pl.edu.agh.sp.tsa.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * A class to load configurations from file 'app.conf'
  *
  * @author Mateusz Kluska
  */
object ConfigLoader {
  private val config: Config = ConfigFactory.load("app.conf")

  /**
    * Twitter consumer key, used for establishing connection with Twitter Streaming API
    */
  val consumerKey: String = config.getString("CONSUMER_KEY")
  /**
    * Twitter consumer secret key, used for establishing connection with Twitter Streaming API
    */
  val consumerSecret: String = config.getString("CONSUMER_SECRET")
  /**
    * Twitter access token, used for establishing connection with Twitter Streaming API
    */
  val accessToken: String = config.getString("ACCESS_TOKEN")
  /**
    * Twitter secret access token, used for establishing connection with Twitter Streaming API
    */
  val accessTokenSecret: String = config.getString("ACCESS_TOKEN_SECRET")

  /**
    * Path to directory where is stored Naive Bayes Model, which was created by SparkNaiveBayesModelCreator
    */
  val naiveBayesModelPath: String = config.getString("NAIVE_BAYES_MODEL_CONF")
  /**
    * Path for file with training data for SparkNaiveBayesModelCreator, we used data from 'www.sentiment140.com'
    */
  val trainingDataPath: String = config.getString("TRAINING_DATA_PATH")

  /**
    * Redis server host
    */
  val redisHost: String = config.getString("REDIS_HOST")
  /**
    * Redis server port
    */
  val redisPort: Int = config.getInt("REDIS_PORT")

  /**
    * HDFS server host
    */
  val hdfsHost: String = config.getString("HDFS_HOST")
  /**
    * HDFS server port
    */
  val hdfsPort: Int = config.getInt("HDFS_PORT")
  /**
    * Path for tweets which HDFS is able to load
    */
  val hdfsTweetsPath: String = config.getString("HDFS_TWEETS_PATH")

}
