import java.io.File

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Collect at least the specified number of tweets into json text files.
  */
object Collect {
  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
   // if (args.length < 3) {
     // System.err.println("Usage: " + this.getClass.getSimpleName +
       // "<outputDirectory> <numTweetsToCollect> <intervalInSeconds> <partitionsEachInterval>")
      //System.exit(1)
    //}
    //val Array(outputDirectory, Utils.IntParam(numTweetsToCollect),  Utils.IntParam(intervalSecs), Utils.IntParam(partitionsEachInterval)) =
      //Utils.parseCommandLineWithTwitterCredentials(args)
    val outputDirectory = "D:/UMKC/Docs/Subjects/PBDM/Project_Bluemix/tweets"
    val numTweetsToCollect = 20000
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
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
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
      .map(gson.toJson(_))

    tweetStream.foreachRDD((rdd, time) => {
      val count = rdd.count()
      if (count > 0) {
        val outputRDD = rdd.repartition(partitionsEachInterval)
        outputRDD.saveAsTextFile(outputDirectory + "/tweets_" + time.milliseconds.toString)
        numTweetsCollected += count
        if (numTweetsCollected > numTweetsToCollect) {
          System.exit(0)
        }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}