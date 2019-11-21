
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TwitterWordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Twitter Stream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    val filters: Seq[String] = Seq("cricket")

    val stream = TwitterUtils.createStream(ssc, None, filters)

    val hashTags = stream.flatMap(tweet => tweet.getText.split(" ").filter(_.startsWith("#")))

    ssc.start()
    ssc.awaitTermination()
  }
}

