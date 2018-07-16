package io.saagie.training.streaming.Twitter


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}

object Twitter {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  case class CLIParams(hdfsMaster: String = "", filters: Array[String] = Array.empty)

  val spark: SparkSession = SparkSession.builder()
    .appName("Twitter Spark Streaming")
    .master("local")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.xml")
    .getOrCreate()

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = runningCount.getOrElse(0) + newValues.sum
    Some(newCount)
  }

  def main(args: Array[String]): Unit = {

    //    val parser = parser("Twitter")

    val config = ConfigFactory.load()
    val consumerKey = config.getString("twitter4j.oauth.consumerKey")
    val consumerSecret = config.getString("twitter4j.oauth.consumerSecret")
    val accessToken = config.getString("twitter4j.oauth.accessToken")
    val accessTokenSecret = config.getString("twitter4j.oauth.accessTokenSecret")


    // Set the system properties so that Twitter4j library used by twitter stream
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val appName = "Twitter"


    val stream = new StreamingContext(spark.sparkContext, Seconds(5))
    val tweets = TwitterUtils.createStream(stream, None, Seq("#FIERSDETREBLEUS"))

    val pairs = tweets.flatMap(status => status.getURLEntities)
      .map(_.getExpandedURL).map(url => if (url.contains("twitter")) ("interne", 1) else ("externe", 1))


    val runningCount = pairs.updateStateByKey[Int](updateFunction _)
    pairs.foreachRDD(rdd => rdd.collect().foreach(println))

    stream.start()
    stream.awaitTermination()
  }

}


