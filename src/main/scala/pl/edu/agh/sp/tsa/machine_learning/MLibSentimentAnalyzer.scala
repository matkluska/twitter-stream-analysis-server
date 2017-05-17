package pl.edu.agh.sp.tsa.machine_learning

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF
import pl.edu.agh.sp.tsa.TweetAnalyzer.isTweetInEnglish
import twitter4j.Status

/**
  * Adapts and invoke machine learning methods, prepare data to further analysis
  *
  * @author Mateusz Kluska
  */
object MLibSentimentAnalyzer {
  /**
    *
    * @return object which can transform document into frequency vector
    */
  def hashingTF = new HashingTF()

  /**
    * Prepare data and compute tweet status content sentiment using Naive Bayes Model
    *
    * @param status    tweet status to analysis
    * @param model     Naive Bayes Model for tweet analysing
    * @param stopWords shared list of english stop words. Taken from NLTK
    * @return one of three value which means:
    *         -1: negative sentiment
    *         0: neutral sentiment(always return for non english tweets)
    *         1: positive sentiment
    */
  def computeSentiment(status: Status, model: NaiveBayesModel, stopWords: Broadcast[List[String]]): Int = {
    val tweetInSingleWords = getNoFuzzyTweetText(status.getText, stopWords.value)
    if (isTweetInEnglish(status)) {
      val sentiment = model.predict(hashingTF.transform(tweetInSingleWords))
      normalizeSentiment(sentiment)
    } else {
      0
    }
  }

  /**
    * Normalize computed sentiment result to three value(negative, neutral, positive)
    *
    * @param sentiment computed sentiment value, straight from Naive Bayes Model predict action
    * @return one of three value which means:
    * -1: negative sentiment
    * 0: neutral sentiment(always return for non english tweets)
    * 1: positive sentiment
    */
  def normalizeSentiment(sentiment: Double): Int = {
    sentiment match {
      case x if x == 0 => -1
      case x if x == 2 => 0
      case x if x == 4 => 1
      case _ => 0
    }
  }

  /**
    * Remove from tweet content fuzzy, noisy data like digits, emojis, special characters etc.
    * Also remove each non-value word like stop words.
    * @param tweetText tweet content text to be cleaned
    * @param stopWords list of stop word which do not have any meaning
    * @return
    */
  def getNoFuzzyTweetText(tweetText: String, stopWords: List[String]): Seq[String] = {
    tweetText.toLowerCase()
      .split("\\W+")
      .filter(_.matches("^[a-zA-Z]+$"))
      .filter(!stopWords.contains(_))
  }
}
