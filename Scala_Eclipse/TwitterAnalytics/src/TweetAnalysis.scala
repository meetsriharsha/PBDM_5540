import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import scala.collection.immutable.ListMap

object TweetAnalysis {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TweetCount")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val tweet = sqlContext.load("D:/UMKC/Docs/Subjects/PBDM/PB_Project/iphone6s.json", "json")
   // tweet.printSchema()
    tweet.registerTempTable("tweets")
    sqlContext.cacheTable("tweets")
    val tweetsObj = sqlContext.sql("SELECT * FROM tweets")
    tweetsObj.count()
    tweetsObj.cache()
    tweetsObj.first()
    println(tweet.select("quoted_status.user.id_str").rdd)
    
  }
}