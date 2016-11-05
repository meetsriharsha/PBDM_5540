import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.util.Properties
import java.sql.DriverManager

object Main extends App{
def main(args: Array[String]) {
val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[3]")
SparkContext sc = new SparkContext(sparkConf)
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val tweets = sqlContext.load("jdbc", Map(
"url" -> "jdbc:db2://bluemix05.bluforcloud.com:50000/BLUDB:user=dash019411;password=2k3n2TR3x3mb;",
"dbtable" -> "DASH019411.NORED"))
tweets.registerTempTable("tweetdata")
tweets.printSchema
tweets.collect
}
}