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
package com.ibm.spark.ibmdataserver

import java.sql.{SQLException, DriverManager, Connection}
import java.util.Properties

import org.apache.spark.Partition
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCPartition, JdbcUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.IBMJDBCRelation

class DefaultSource extends CreatableRelationProvider with DataSourceRegister with RelationProvider{

  override def shortName(): String = Constants.SOURCENAME

  Class.forName("com.ibm.db2.jcc.DB2Driver");

  private def createTableIfNotExist(url: String, table: String, mode: SaveMode,
                                    props: Properties, dataFrame: DataFrame) : Unit = {

    val conn : Connection = JdbcUtils.createConnection(url, props)

    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)

      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(dataFrame, url)
        val sql = s"CREATE TABLE $table ($schema)"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } catch {
          case sqle: SQLException => throw sqle
        } finally {
          statement.close()
        }
      }
    } catch {
      case sqle: SQLException => throw sqle
    } finally {
      conn.close()
    }
  }


  private def getNumberOfPartitions(url: String, table: String, props: Properties):
              Array[Partition] = {

    val conn : Connection = JdbcUtils.createConnection(url, props)
    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)
      if (!tableExists) {
        sys.error(s"Table $table does not exist.")
      }

      val stmt = conn.createStatement();
      try {
        // simplifying assumption that all tables are partitioned across all members
        // would need lookup of relevant members via tablespace and partition group here
        val rs1 = stmt.executeQuery(
          "select max(partition_number) from table(sysproc.db_partitions()) as t")
        require(rs1.next)
        val maxparts = rs1.getInt(1)

        // just need any column from the table.
        // unfortunately we don't have the RDD schema at this point
        val rs2 = stmt.executeQuery("select colname from syscat.columns where tabschema = current schema " +
          s"and tabname = '${table.toUpperCase}' and partkeyseq = 1")
        var partKeyColName = ""

        if(maxparts > 0) {
          // require(rs2.next, s"Table $table is not MPP partitioned")
          require(rs2.next, s"Table partition column info could not be retrieved")
          partKeyColName = rs2.getString(1)
        }

        IBMJDBCRelation.buildPartitionArray(maxparts, partKeyColName)
      } finally {
        stmt.close()
      }
    } finally {
      conn.close()
    }
  }

  private def getURLFromParams(parameters: Map[String, String]): String = {
    parameters.getOrElse(Constants.JDBCURL,
      sys.error("Option 'JDBCURL' [Constants.JDBCURL] not specified"))
  }

  private def getTableNameFromParams(parameters: Map[String, String]): String = {
    parameters.getOrElse(Constants.TABLE,
      sys.error("Option 'TABLE' [Constants.TABLE] not specified"))
  }

  private def getTmpPathFromParams(parameters: Map[String, String]): String = {
    val systemTempPath = System.getProperty("java.io.tmpdir")
    var tmpPath = parameters.getOrElse(Constants.TMPPATH, systemTempPath)

    if(tmpPath.isEmpty) {
      tmpPath = systemTempPath
    }
    return tmpPath
  }

  private def getIsRemoteFromParams(parameters: Map[String, String]): Boolean = {
    java.lang.Boolean.valueOf(parameters.getOrElse(Constants.ISREMOTE, "false"))
  }

  private def getParallelismFromParams(parameters: Map[String, String]): Int = {
    var parallelism = parameters.getOrElse(Constants.PARALLELISM, "2").toInt
    if(parallelism <= 0) {
      parallelism = 2
    }
    return parallelism
  }

  private def getPropertiesFromParams(parameters: Map[String, String]): Properties = {
    val props: Properties = new Properties()

    parameters.foreach(element => {
      props.put(element._1, element._2)
    })

    return props
  }

  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val url: String = getURLFromParams(parameters)
    val table: String = getTableNameFromParams(parameters)
    var tmpPath: String = getTmpPathFromParams(parameters)
    val isRemote: java.lang.Boolean = getIsRemoteFromParams(parameters)
    val props: Properties = getPropertiesFromParams(parameters)
    var parallelism: Int = getParallelismFromParams(parameters)

    createTableIfNotExist(url, table, mode, props, data)

    val parts = getNumberOfPartitions(url, table, props)
    val relation = new IBMJDBCRelation(url, table, parts, props)(sqlContext)
    relation.saveTableData(data, parallelism, tmpPath, isRemote)
    return relation
  }

  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val url = getURLFromParams(parameters)
    val table = getTableNameFromParams(parameters)
    // Additional properties that we will pass to getConnection
    val props: Properties = getPropertiesFromParams(parameters)

    val parts = getNumberOfPartitions(url, table, props)
    new IBMJDBCRelation(url, table, parts, props)(sqlContext)
  }
}