/**
  * (C) Copyright IBM Corp. 2016
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
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
      // .option(Constants.TABLE, tableName)
      .option("user", "pallavipr")
      .option("password", "9manjari")
      .option("dbtable", "employee")
      .load()

    jdbcRdr.show()
  }
}
