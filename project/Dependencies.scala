import sbt._

object Dependencies {
  val sparkVersion = "2.1.0"
  val sparkStreamingTwitterVersion = "1.6.3"
  val configVersion = "1.3.0"
  val jedisVersion = "2.9.0"

  val projectDependencies = Seq(
    "com.typesafe" % "config" % configVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
    "redis.clients" % "jedis" % jedisVersion
  )
}