import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import scala.collection.immutable.ListMap

object TweetCount {
  
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TweetCount")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val tweets = sc.textFile("K:/UMKC/Docs/Subjects/PBDM/PB_Project/iphone6s.json")
    //.map(gson.toJson(_))
   // println(tweets.first())
    //tweets.collect()
    //val userMap = tweets.map(x => ((extractUserId(x),extractLang(x)), 1))
    val userTweetCnt = tweets.filter(_.nonEmpty).map(x => (extractUserId(x), 1))
    val langTweetCnt = tweets.filter(_.nonEmpty).map(x => (extractLang(x), 1))
    val tweetCreatedDt = tweets.filter(_.nonEmpty).map(x => (extractTweetDate(x), 1))
    val tweetCreatedDtLang = tweets.filter(_.nonEmpty).map(x => ((extractTweetDate(x),extractLang(x)), 1))
    val numTweetsPerUser = userTweetCnt.reduceByKey((a, b) => a + b)
    val numTweetsPerLang = langTweetCnt.reduceByKey((a, b) => a + b)
    val tweetCreatedDtCnt = tweetCreatedDt.reduceByKey((a, b) => a + b)
    val tweetCreatedDtLangCnt = tweetCreatedDtLang.reduceByKey((a, b) => a + b)
    val numTweetsPerUserSorted =numTweetsPerUser.sortBy(_._2)
    val numTweetsPerLangSorted =numTweetsPerLang.sortBy(_._2)
    numTweetsPerUser.repartition(1).saveAsTextFile("K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerUser")
    numTweetsPerLang.repartition(1).saveAsTextFile("K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerLang")
    tweetCreatedDtCnt.repartition(1).saveAsTextFile("K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerTime")
    tweetCreatedDtLangCnt.repartition(1).saveAsTextFile("K:/UMKC/Docs/Subjects/PBDM/PB_Project/TweetsPerTimeLang")
//    val tweet = sqlContext.load("D:/UMKC/Docs/Subjects/PBDM/PB_Project/iphone6s.json", "json")
  //  tweet.printSchema()
    //println(tweet.select("quoted_status.user.id_str").rdd)
    //val userMp = tweet.map { x => ( }
  }

  def extractUserId(x: String): String = {
    val userIdPattern = "\"user\"\\:\\{\"id\"\\:([^,]+)".r
    val userId = userIdPattern.findFirstMatchIn(x).map(x => x.group(1)).getOrElse("na")
    userId
  }
  def extractLang(x: String): String = {
  //val userIdPattern = "\"user\".*\"id\"\\:([^,]+)".r
  //val userIdPattern = "\"user\"\\:\\{\"id\"\\:([^,]+)".r
    val langPattern = "\"user\"\\:\\{\"id\".*\"lang\"\\:([^,]+)".r
    val lang = langPattern.findFirstMatchIn(x).map(x => x.group(1)).getOrElse("na")
    lang
  }
  def extractFollowerCnt(x: String): String = {
  //val userIdPattern = "\"user\".*\"id\"\\:([^,]+)".r
  //val userIdPattern = "\"user\"\\:\\{\"id\"\\:([^,]+)".r
    val followerCntPattern = "\"user\"\\:\\{\"id\".*\"followers_count\"\\:([^,]+)".r
    val followerCnt = followerCntPattern.findFirstMatchIn(x).map(x => x.group(1)).getOrElse("na")
    followerCnt
  }
  def extractTweetDate(x: String): String = {
  //val userIdPattern = "\"user\".*\"id\"\\:([^,]+)".r
  //val userIdPattern = "\"user\"\\:\\{\"id\"\\:([^,]+)".r
    val tweetDatePattern = "\"created_at\"\\:([^,]+)".r
    val tweetDate = tweetDatePattern.findFirstMatchIn(x).map(x => x.group(1)).getOrElse("na")
    tweetDate
  }
}