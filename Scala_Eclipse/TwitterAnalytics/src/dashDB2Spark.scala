import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf 
import org.apache.spark.SparkContext

object DB2Spark extends App {
val conf = new SparkConf()
.setMaster("local[1]")
.setAppName("GetEmployee")
.set("spark.executor.memory", "1g")

val sc = new SparkContext(conf)

val sqlContext = new SQLContext(sc)

val employeeDF = sqlContext.load("jdbc", Map(
"url" -> "jdbc:db2://bluemix05.bluforcloud.com:50000/BLUDB:user=dash019411;password=2k3n2TR3x3mb;",
"driver" -> "com.ibm.db2.jcc.DB2Driver",
"dbtable" -> "DASH019411.NORED"))

employeeDF.show(10000);
employeeDF.collect();
}