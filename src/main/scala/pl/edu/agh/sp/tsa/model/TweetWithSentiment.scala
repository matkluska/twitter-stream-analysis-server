package pl.edu.agh.sp.tsa.model

import scala.util.parsing.json.JSONObject

class TweetWithSentiment(statusId: Long, userName: String, tweetText: String, sentiment: Int,
                         latitude: Double, longitude: Double, avatarURL: String, creationDate: String) {

  def toJSONObject: JSONObject = {
    JSONObject.apply(Map(
      "statusId" -> statusId,
      "userName" -> userName,
      "tweetText" -> tweetText,
      "fillKey" -> sentiment,
      "latitude" -> latitude,
      "longitude" -> longitude,
      "avatarURL" -> avatarURL,
      "creationDate" -> creationDate
    ))
  }

}
