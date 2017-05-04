package pl.edu.agh.sp.tsa.model

import scala.util.parsing.json.JSONObject

class Tweet(var text: String) {

  def toJSONObject: JSONObject = {
    val map: Map[String, Any] = Map(
      "text" -> text
    )

    JSONObject.apply(map)
  }

}
