package pl.edu.agh.sp.tsa.machine_learning

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import pl.edu.agh.sp.tsa.util.ConfigLoader

import scala.io.Source

/**
  * An object responsible for creating Spark Naive Bayes Model which should has ability to predict tweet sentiment.
  *
  * @author Mateusz Kluska
  */
object SparkNaiveBayesModelCreator {
  /** Starts Spark Naive Bayes Model creation
    *
    * @param args not used
    */
  def main(args: Array[String]): Unit = {
    val spark = getOrCreateSparkSession()

    val stopWordsBroadcast = spark.sparkContext.broadcast(loadStopWordsFromFile())

    createAndSaveNaiveBayesClassifier(stopWordsBroadcast)
  }

  /**
    * Create or get existing Spark Session.
    * Here could be configure spark cluster, we set local processor with four cores, but it could much more efficient machine
    *
    * @return configured Spark Session object
    */
  def getOrCreateSparkSession(): SparkSession = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)

    SparkSession.builder()
      .config(conf)
      .master("local[4]")
      .getOrCreate()
  }

  /**
    * Load stop words from file, and converted it to list of strings
    *
    * @return list of stop words
    */
  def loadStopWordsFromFile(): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/NLTK_english_stop_words.txt")).getLines().toList
  }

  /**
    * Create, train Naive Bayes Model and save it to file which path is set in app.conf under 'NAIVE_BAYES_MODEL_CONF' label.
    * Method used machine learning ideas like text frequency vector, labeling.
    * Created model is trained with lambda parameter set to 1.0 and
    * data from file which path is set in app.conf under 'TRAINING_DATA_PATH' label.
    *
    *
    * @param stopWordsBroadcast shared list of english stop words. Taken from NLTK
    */
  def createAndSaveNaiveBayesClassifier(stopWordsBroadcast: Broadcast[List[String]]): Unit = {
    val tweets = loadTrainOrTestDataFile(ConfigLoader.trainingDataPath)
    val hashingTF = new HashingTF()

    val labeledTweets = tweets.select("sentiment", "status").rdd.map {
      case Row(sentiment: Int, status: String) =>
        val tweetSingleWords = MLibSentimentAnalyzer.getNoFuzzyTweetText(status, stopWordsBroadcast.value)
        hashingTF.transform(tweetSingleWords)
        LabeledPoint(sentiment, hashingTF.transform(tweetSingleWords))
    }
    labeledTweets.cache()

    val naiveBayesModel = NaiveBayes.train(labeledTweets, lambda = 1.0)
    naiveBayesModel.save(getOrCreateSparkSession().sparkContext, ConfigLoader.naiveBayesModelPath)
  }

  /**
    * Load train or test data which should be in csv format and get only valuable information which are sentiment and tweet text
    * We used training data from www.sentiment140.com site
    *
    * @param filePath absolute path for file with data
    * @return
    */
  def loadTrainOrTestDataFile(filePath: String): DataFrame = {
    val spark = getOrCreateSparkSession()

    spark.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(filePath)
      .toDF("sentiment", "id", "date", "query", "user", "status")
      .drop("id").drop("date").drop("query").drop("user")
  }

}
