package pl.edu.agh.sp.tsa.util

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by mateusz on 23/04/17.
  */
object ConfigLoader {
  private val config: Config = ConfigFactory.load("app.conf")

  val consumerKey: String = config.getString("CONSUMER_KEY")
  val consumerSecret: String = config.getString("CONSUMER_SECRET")
  val accessToken: String = config.getString("ACCESS_TOKEN")
  val accessTokenSecret: String = config.getString("ACCESS_TOKEN_SECRET")

}
