import java.io.File

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * Collect at least the specified number of tweets into json text files.
 */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    val outputDirectory = "D:/UMKC/Docs/Subjects/PBDM/Project_Bluemix/tweets"
    val numTweetsToCollect = 200000
    val intervalSecs = 30
    val partitionsEachInterval = 1
   
    val outputDir = new File(outputDirectory.toString)
    if (outputDir.exists()) {
      System.err.println("ERROR - %s already exists: delete or specify another directory".format(
        outputDirectory))
      System.exit(1)
    }
    outputDir.mkdirs()

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalSecs))
    val consumerKey = "dF7iP9i3cxpcLeDqAaoaBjjvT";
   val consumerSecret = "XzxL2WoE2dCTOZNAGnmXI1w4VYgyJNhO4t02DjeK7mEq2M4D4w";
   val accessToken = "86457010-pJp5znqBdUIm8pvspVYfxUyfYarmh1WwkFAfHrWMX";
   val accessTokenSecret = "5c3Au47cUJvi0OnDAj7YeQUBDbLjGvXo8l7PHFqnLHwhP";
   System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
   System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
   System.setProperty("twitter4j.oauth.accessToken", accessToken)
   System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
   val filters = Array("#android")
    val tweetStream = TwitterUtils.createStream(ssc, None, filters)
    //.map(gson.toJson(_))
      
   
    tweetStream.saveAsTextFiles("tweets")

    ssc.start()
    ssc.awaitTermination()
  }
}