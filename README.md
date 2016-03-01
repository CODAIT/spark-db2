# DB2 Hi-Speed connector for Spark

A library for fast loading and unloading of data between SPARK and DB2.

## Requirements

This library requires Spark 1.6+ and Apache Commons CSV library

## What the package does
This package provides option for persisting data from SPARK into DB2 at HiSpeed leveraging the DB2 LOAD utility and to retrieve data from DB2 into SPARK leveraging the capabilities of DB2 partition feature.

### Scala API

Load data from DB2 into Spark DataFrame
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

Persist Spark DataFrame into DB2
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

Load data from DB2 into Spark DataFrame
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

Persist Spark DataFrame into DB2
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