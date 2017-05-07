package pl.edu.agh.sp.tsa.model

import scala.util.parsing.json.{JSONArray, JSONObject}

class Tweet(var text: String, var retweetCount: Long, var favoriteCount: Long, var hashtags: List[String]) {

  def toJSONObject: JSONObject = {
    JSONObject.apply(Map(
      "text" -> text,
      "retweetCount" -> retweetCount,
      "favoriteCount" -> favoriteCount,
      "hashtags" -> JSONArray.apply(hashtags)
    ))
  }

}
