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
package org.apache.spark.sql

import java.io.FileWriter
import java.lang.Float
import java.sql.{SQLException, Connection}
import java.util.{NoSuchElementException, ArrayList, Properties, List}

import com.ibm.spark.ibmdataserver.Constants
import org.apache.commons.csv.{CSVPrinter, CSVFormat}
import org.apache.spark.Partition
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils._
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JdbcUtils, JDBCRelation}
import org.apache.spark.sql.jdbc.{JdbcType, JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._
import org.apache.spark.Logging

import scala.util.Random

class IBMJDBCRelation(url: String,
                      table: String,
                      parts: Array[Partition],
                      properties: Properties = new Properties()) (sqlContext: SQLContext)
  extends JDBCRelation(url, table, parts, properties)(sqlContext)
  with Logging {

  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  def createConnection(url: String, properties: Properties) : () => Connection = {
    () => {
      JdbcUtils.createConnection(url, properties)
    }
  }

  private def savePartition(
                             getConnection: () => Connection,
                             table: String,
                             iterator: Iterator[Row],
                             rddSchema: StructType,
                             nullTypes: Array[Int],
                             batchSize: Int,
                             dialect: JdbcDialect,
                             parallelism: Int) = {
    /*
      Duplicate the iterator so that we get the length of elements in the iterator and be able to chunk them into
      equal sizes.
      Note: calling length on iterator moves it to last position of the element hence the duplicate
     */
    val itDuplicates = iterator.duplicate
    val length = itDuplicates._1.length + 1 // Length is zero indexed
    val chunk: Int = Math.ceil(length.toDouble/parallelism).toInt

    /*
      Spawn 'parallelism' number of threads with each inserting data into the table from chunks picked up by it
     */
    val insertThreads = for ( pCount <- 1 to parallelism toList ) yield {
      new Thread() {
        override def run() {
          val conn = getConnection()
          var committed = false

          var chunkIterator: Iterator[Row] = null
          this.synchronized {
            chunkIterator = itDuplicates._2.take(chunk)
          }

          try {
            conn.setAutoCommit(false) // Everything in the same db transaction.
            val stmt = insertStatement(conn, table, rddSchema)
            try {
              var rowCount = 0
              while (chunkIterator.hasNext) {
                val row = chunkIterator.next()
                val numFields = rddSchema.fields.length
                var i = 0
                while (i < numFields) {
                  if (row.isNullAt(i)) {
                    stmt.setNull(i + 1, nullTypes(i))
                  } else {
                    rddSchema.fields(i).dataType match {
                      case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                      case LongType => stmt.setLong(i + 1, row.getLong(i))
                      case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                      case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                      case ShortType => stmt.setInt(i + 1, row.getShort(i))
                      case ByteType => stmt.setInt(i + 1, row.getByte(i))
                      case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                      case StringType => stmt.setString(i + 1, row.getString(i))
                      case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                      case TimestampType => stmt.setTimestamp(i + 1,
                        row.getAs[java.sql.Timestamp](i))
                      case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                      case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                      case ArrayType(et, _) =>
                        val array = conn.createArrayOf(
                          getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase,
                          row.getSeq[AnyRef](i).toArray)
                        stmt.setArray(i + 1, array)
                      case _ => throw new IllegalArgumentException(
                        s"Can't translate non-null value for field $i")
                    }
                  }
                  i = i + 1
                }
                stmt.addBatch()
                rowCount += 1
                if (rowCount % batchSize == 0) {
                  stmt.executeBatch()
                  rowCount = 0
                  conn.commit()
                  committed = true
                }
              }
              if (rowCount > 0) {
                stmt.executeBatch()
                conn.commit()
                committed = true
              }
            } catch {
              case sqle: SQLException => throw sqle.getNextException
              // TODO Add logging
              case exception : NoSuchElementException => println("Hit empty iter situation") // scalastyle:ignore
            } finally {
              stmt.close()
            }
            /* conn.commit()
            committed = true */
          } finally {
            if (!committed) {
              // The stage must fail.  We got here through an exception path, so
              // let the exception through unless rollback() or close() want to
              // tell the user about another problem.
              conn.rollback()
              conn.close()
            } else {
              // The stage must succeed.  We cannot propagate any exception close() might throw.
              try {
                conn.close()
              } catch {
                case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
              }
            }
          }
        }
      }
    }

    insertThreads.foreach(thread => thread.start())
    insertThreads.foreach(thread => thread.join())
  }

  private def savePartition(
                             getConnection: () => Connection,
                             table: String,
                             iterator: Iterator[Row],
                             rddSchema: StructType,
                             nullTypes: Array[Int],
                             batchSize: Int,
                             dialect: JdbcDialect,
                             path: String) = {
    val randomInt: Int = Math.abs(Random.nextInt())
    val fileName = path + "/" + randomInt + ".csv"
    val scriptfileName = path + "/" + randomInt + ".sql"
    val fileWriter: FileWriter = new FileWriter(fileName)
    val fileFormat: CSVFormat = CSVFormat.DEFAULT.withRecordSeparator("\n")
    val csvFilePrinter: CSVPrinter = new CSVPrinter(fileWriter, fileFormat)

    /*
       Iterate through the dataFrame in this partition and write out
       the data to a csv file which will be used by DB2
       Utility to load data into the table
    */
    try {
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val csvRow: List[Object] = new ArrayList[Object]()
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              csvRow.add(null)
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => csvRow.add(new Integer(row.getInt(i)))
                case LongType => csvRow.add(new java.lang.Long(row.getLong(i)))
                case DoubleType => csvRow.add(new java.lang.Double(row.getDouble(i)))
                case FloatType => csvRow.add(new Float(row.getFloat(i)))
                case ShortType => csvRow.add(new java.lang.Short(row.getShort(i)))
                case ByteType => csvRow.add(new java.lang.Byte(row.getByte(i)))
                case BooleanType => var value: Boolean = row.getBoolean(i)
                  if(value == true) {
                    csvRow.add(new  Integer(1))
                  } else {
                    csvRow.add(new Integer(0))
                  }
                case StringType => csvRow.add(row.getString(i))
                case t: DecimalType => csvRow.add(row.getDecimal(i))
                // case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                // case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                // case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                /* case ArrayType(et, _) =>
                       val array = conn.createArrayOf(
                         getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase,
                         row.getSeq[AnyRef](i).toArray)
                       stmt.setArray(i + 1, array) */
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          csvFilePrinter.printRecord(csvRow)
          rowCount += 1
        }
      } catch {
        case sqle: SQLException => throw sqle.getNextException
      }
    } finally {
      fileWriter.flush()
      fileWriter.close()
      csvFilePrinter.close()
    }

    val conn = getConnection()
    val columns = rddSchema.fields.map(_.name).mkString(",")
    /*try {
      val sql = "call SYSPROC.ADMIN_CMD(' load from " + fileName +
        " of DEL insert into " + table + "( " + columns + " )')"
      val stmt = conn.prepareStatement(sql)
      stmt.execute()
    } catch {
      case sqle: SQLException => throw sqle
    } finally {
      conn.close()
    }*/

    val scriptfileWriter: FileWriter = new FileWriter(scriptfileName)
    scriptfileWriter.write("connect to sparkdb;\n")
    //scriptfileWriter.write("ingest from file " + fileName + " format delimited insert into " + table + ";")
    scriptfileWriter.write("load client from " + fileName + " of DEL insert into " + table + "( " + columns + " ) ;")
    scriptfileWriter.flush()
    scriptfileWriter.close()

    //Call DB2 utility to load data into table
    //val proc:Process = Runtime.getRuntime.exec("db2 \"connect to sparkdb\" && " + "db2 'ingest from file " + fileName + "format delimited insert into " + table + "'" )
    val proc:Process = Runtime.getRuntime.exec("db2 -tvsf  "+ scriptfileName)
    proc.waitFor()
    println(scala.io.Source.fromInputStream(proc.getInputStream).getLines().mkString("\n"))
  }

  def saveTableData(dataFrame: DataFrame, parallelism: Int, path: String, isRemote: Boolean) = {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = dataFrame.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = dataFrame.schema
    val getConnection: () => Connection = createConnection(url,new Properties())
    val batchSize = properties.getProperty(Constants.BATCHSIZE, "1000").toInt

    if(isRemote) {
      dataFrame.foreachPartition { iterator =>
        savePartition(getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect,parallelism)
      }
    } else {
      dataFrame.foreachPartition { iterator =>
        savePartition(getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect,path)
      }
    }
  }
}

object IBMJDBCRelation {
  def buildPartitionArray(maxparts: Int, partKeyColName: String): Array[Partition] = {
    if (maxparts == 0) {
      Array[Partition](JDBCPartition(null, 0))
    } else {
      (0 to maxparts) map { i => JDBCPartition(s"dbpartitionnum($partKeyColName)=$i", i) } toArray
    }
  }
}