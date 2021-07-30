This project is not being actively mantained and has been archived. 

# DB2/DashDB Hi-Speed connector for Apache Spark

A library for fast loading and unloading of data between Apache Spark and DB2/DashDB.

## Requirements

This library requires Apache Spark 1.6+, Apache Commons CSV library and DB2 JDBC driver.

## What the package does
This package provides options for:

1) Persisting data from Apache Spark into DB2/DashDB at HiSpeed leveraging the DB2 LOAD utility.

2) Loading of data from DB2/DashDB into Apache Spark using parallel read from database partitions.

## Build procedure

```
git clone https://github.com/SparkTC/spark-db2
cd spark-db2
sbt clean package
```

### Scala API

Load data from DB2/DashDB into Apache Spark using DataFrames API
```scala
import com.ibm.spark.ibmdataserver.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

val sparkContext = new SparkContext(conf)
val sqlContext = new SQLContext(sparkContext)
val df = sqlContext.read
    .format("com.ibm.spark.ibmdataserver")
    .option(Constants.JDBCURL, DB2_CONNECTION_URL) //Specify the JDBC connection URL
    .option(Constants.TABLE,tableName) //Specify the table from which to read
    .load()
df.show()
```

Persist data from Apache Spark DataFrames into DB2/DashDB
```scala
import com.ibm.spark.ibmdataserver.Constants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

val sparkContext = new SparkContext(conf)
val sqlContext = new SQLContext(sparkContext)

val df = sqlContext.read.json("/Users/sparkuser/data.json")
df.write
    .format("com.ibm.spark.ibmdataserver")
    .option(Constants.JDBCURL, DB2_CONNECTION_URL) //Specify the JDBC connection URL
    .option(Constants.TABLE,tableName) //Specify the table (will be created if not present) to which data is to be written
    .option(Constants.TMPPATH, tmpPath) //Temporary path to be used for generating intermediate files during processing [System tmp path will be used by default]
    .mode("Append")
    .save()
```

### Java API

Load data from DB2 into Apache Spark using DataFrames API
```java
import com.ibm.spark.ibmdataserver.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("write test");
SparkContext sparkContext = new SparkContext(conf);
SQLContext sqlContext = new SQLContext(sparkContext);

DataFrame df = sqlContext.read().format("com.ibm.spark.ibmdataserver")
                .option(Constants.JDBCURL(), DB2_CONNECTION_URL) //Specify the JDBC connection URL
                .option(Constants.TABLE(),tableName)
                .load();
df.show()
```

Persist data from Apache Spark DataFrames into DB2
```java
import com.ibm.spark.ibmdataserver.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("write test");
SparkContext sparkContext = new SparkContext(conf);
SQLContext sqlContext = new SQLContext(sparkContext);

DataFrame df = sqlContext.read().json(path);
df.write()
    .format("com.ibm.spark.ibmdataserver") //Specify the table from which to read
    .option(Constants.JDBCURL(), DB2_CONNECTION_URL) //Specify the JDBC connection URL
    .option(Constants.TABLE(),tableName) //Specify the table (will be created if not present) to which data is to be written
    .option(Constants.TMPPATH(), tmpPath) //Temporary path to be used for generating intermediate files during processing [System tmp path will be used by default]
    .mode("Append")
    .save();
```
