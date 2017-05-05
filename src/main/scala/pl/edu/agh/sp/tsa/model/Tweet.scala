package pl.edu.agh.sp.tsa.model

import scala.util.parsing.json.{JSONArray, JSONObject}

class Tweet(var text: String, var retweetCount: Long, var hashtags: List[String]) {

  def toJSONObject: JSONObject = {
    val map: Map[String, Any] = Map(
      "text" -> text,
      "retweetCount" -> retweetCount
//      "hashtags" -> JSONArray.apply(hashtags)
    )

    JSONObject.apply(map)
  }

}
