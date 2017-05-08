package pl.edu.agh.sp.tsa.model

import scala.util.parsing.json.JSONObject

/**
  * A class to represent a ''tweet'' with analyzed sentiment
  *
  * @author Mateusz Kluska
  * @param statusId tweet status Id (original value from ''Twitter'')
  * @param userName tweet author name
  * @param tweetText tweet text content
  * @param sentiment tweet computed sentiment(-1: negative, 0: neutral, 1: positive)
  * @param latitude tweet location latitude
  * @param longitude tweet geo location longitude
  * @param avatarURL tweet author profile picture URL
  * @param creationDate tweet creation Date
  */
class TweetWithSentiment(statusId: Long, userName: String, tweetText: String, sentiment: Int,
                         latitude: Double, longitude: Double, avatarURL: String, creationDate: String) {

  /**
    * A method which convert the class instance to JSONObject, due to this tweet with sentiment could be easily read by frontend side
    *
    * JSON format:
    * {{{
    * {
    *   "statusId": number,
    *   "userName": string,
    *   "tweetText": string,
    *   "keyFill": string,
    *   "latitude": number,
    *   "longitude": number,
    *   "avatarURL": string,
    *   "creationDate": string
    * }
    * }}}
    *
    * @return JSONObject
    */
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
