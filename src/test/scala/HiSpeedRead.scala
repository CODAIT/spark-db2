/**
  * Created by pallavipr on 2/22/2016.
  */

import com.ibm.spark.ibmdataserver.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object HiSpeedRead {

  def main(args: Array[String]) {
    val DB2_CONNECTION_URL = "jdbc:db2://localhost:50700/sample:traceFile=C:\\1.txt;"

    val conf = new SparkConf().setMaster("local[2]").setAppName("read test")

    val sparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sparkContext)

    Class.forName("com.ibm.db2.jcc.DB2Driver")

    val jdbcRdr = sqlContext.read.format("com.ibm.spark.ibmdataserver")
      .option("url", DB2_CONNECTION_URL)
      //.option(Constants.TABLE, tableName)
      .option("user", "pallavipr")
      .option("password", "9manjari")
      .option("dbtable", "employee")
      .load()

    jdbcRdr.show()
  }
}
