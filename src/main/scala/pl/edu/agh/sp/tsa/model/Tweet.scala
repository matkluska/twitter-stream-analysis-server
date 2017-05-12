package pl.edu.agh.sp.tsa.model

import scala.util.parsing.json.JSONObject

/**
  * A class to represent a ''tweet'' from HDFS
  *
  * @author Mateusz Nowak
  * @param id       tweet ID
  * @param username tweet author name
  */
class Tweet(var id: String, var username: String) {

  /**
    * A method which convert the class instance to JSONObject
    *
    * JSON format:
    * {{{
    * {
    *   "id": string,
    *   "username": string,
    * }
    * }}}
    *
    * @return JSONObject
    */
  def toJSONObject: JSONObject = {
    JSONObject.apply(Map(
      "id" -> id,
      "username" -> username
    ))
  }

}
