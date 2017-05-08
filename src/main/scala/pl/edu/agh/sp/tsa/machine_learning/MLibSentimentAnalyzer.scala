package pl.edu.agh.sp.tsa.machine_learning

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import pl.edu.agh.sp.tsa.TweetAnalyzer.{hashingTF, isTweetInEnglish}
import twitter4j.Status

/**
  * Created by mateusz on 08/05/17.
  */
object MLibSentimentAnalyzer {
  def computeSentiment(status: Status, model: NaiveBayesModel, stopWords: Broadcast[List[String]]): Int = {
    val tweetInSingleWords = getNoFuzzyTweetText(status.getText, stopWords.value)
    if (isTweetInEnglish(status)) {
      val sentiment = model.predict(hashingTF.transform(tweetInSingleWords))
      normalizeSentiment(sentiment)
    } else {
      0
    }
  }

  def normalizeSentiment(sentiment: Double): Int = {
    sentiment match {
      case x if x == 0 => -1
      case x if x == 2 => 0
      case x if x == 4 => 1
      case _ => 0
    }
  }

  def getNoFuzzyTweetText(tweetText: String, stopWords: List[String]): Seq[String] = {
    tweetText.toLowerCase()
      .split("\\W+")
      .filter(_.matches("^[a-zA-Z]+$"))
      .filter(!stopWords.contains(_))
  }
}
