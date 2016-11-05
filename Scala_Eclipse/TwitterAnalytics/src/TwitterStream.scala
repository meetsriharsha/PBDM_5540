import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.util.Properties
import java.sql.DriverManager
import java.sql.PreparedStatement

object TwitterStream extends App{
   Class.forName("com.ibm.db2.jcc.DB2Driver")
   val con = DriverManager.getConnection("jdbc:db2://bluemix05.bluforcloud.com:50000/BLUDB:user=dash019411;password=2k3n2TR3x3mb;")
   val SQL="insert into DASH019411.TWITTERUSERS values (?)"
   val ps = con.prepareStatement(SQL)
    
   val consumerKey = "dF7iP9i3cxpcLeDqAaoaBjjvT";
   val consumerSecret = "XzxL2WoE2dCTOZNAGnmXI1w4VYgyJNhO4t02DjeK7mEq2M4D4w";
   val accessToken = "86457010-pJp5znqBdUIm8pvspVYfxUyfYarmh1WwkFAfHrWMX";
   val accessTokenSecret = "5c3Au47cUJvi0OnDAj7YeQUBDbLjGvXo8l7PHFqnLHwhP";
   System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
   System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
   System.setProperty("twitter4j.oauth.accessToken", accessToken)
   System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

   val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[3]")
   val ssc = new StreamingContext(sparkConf, Seconds(2))
   
   val filters = Array("#android")
   val stream = TwitterUtils.createStream(ssc, None, filters) 
   val users = stream.map(status => status.getUser.getName)
   val recentUsers = users.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
  
   recentUsers.foreachRDD(rdd => {
      println("\nNumber of users in last 60 seconds (%s total):".format(rdd.count()))
      rdd.foreach{
        case (user, tag) => println("%s ".format(user))
        val singleUser = format(user)
        ps.setString(1, singleUser)
        ps.execute()
        println("Inserted Twitter User into DB: " + singleUser)
        }
    })
    ssc.start()
    ssc.awaitTermination()
}