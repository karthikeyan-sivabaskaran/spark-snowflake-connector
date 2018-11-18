

package connector

import org.apache.spark.sql.SparkSession
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.log4j.Level
import org.apache.log4j.Logger
import net.snowflake.spark.snowflake.Utils

object sparkSnowflakeConnector extends sfCredentials with App {

  println("spark snowflake connector program started ...")
  Logger.getLogger("org").setLevel(Level.ERROR)

//  val sfOptions = Map(
//    "sfURL" -> "xy12345.snowflakecomputing.com",
//    "sfAccount" -> "xy12345",
//    "sfUser" -> "karthik",
//    "sfPassword" -> "password",
//    "sfDatabase" -> "database",
//    "sfSchema" -> "myschema",
//    "sfWarehouse" -> "my_warehouse")

  val spark = SparkSession.builder
    .master("local")
    .appName("spark snowflake connector")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  println("reading snowflake table:")

  val df = spark.read
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "COMPACTED_TBL")
    .load().cache

  println("printing schema ...")

  df.printSchema

  println("snowflake table:")
  println("***************")

  df.show(false)
  
    println("writing ECLIPSE_TABLE to snowflake....")
    df.write
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "SPARK_SNOWFLAKE_TABLE")
    .mode("overwrite")
    .save()
    
    println("SPARK_SNOWFLAKE_TABLE write completed.....")
    println("reading SPARK_SNOWFLAKE_TABLE from snowflake...")
  
    val eclipse_tbl = spark.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", "SPARK_SNOWFLAKE_TABLE")
      .load().cache
  
    println("SPARK_SNOWFLAKE_TABLE:")
    println("*********************")
  
    eclipse_tbl.show(false)
    
    val drop_tbl_query = "drop table if exists SF_TABLE"
    val create_tbl_query = "create table SF_TABLE as select * from SPARK_SNOWFLAKE_TABLE"
    
    Utils.runQuery(sfOptions, drop_tbl_query)        //these queries directly run inside snowflake
    Utils.runQuery(sfOptions, create_tbl_query)  
    
    println("SF_TABLE created successfully")

}