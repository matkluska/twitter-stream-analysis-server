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
  * Created by mateusz on 02/05/17.
  */
object SparkNaiveBayesModelCreator {
  def main(args: Array[String]): Unit = {
    val spark = getOrCreateSparkSession()

    val stopWordsBroadcast = spark.sparkContext.broadcast(loadStopWordsFromFile())

    createAndSaveNaiveBayesClassifier(stopWordsBroadcast)
  }

  def getOrCreateSparkSession(): SparkSession = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)

    SparkSession.builder()
      .config(conf)
      .master("local[4]")
      .getOrCreate()
  }

  def loadStopWordsFromFile(): List[String] = {
    Source.fromInputStream(getClass.getResourceAsStream("/NLTK_english_stop_words.txt")).getLines().toList
  }

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
