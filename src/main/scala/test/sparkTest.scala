package test

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }

object sparkTest extends constants with App{
  

  Logger.getLogger("org").setLevel(Level.ERROR)

  println("hello ulagam !!")
  
  println(testdata)
  
  val spark = SparkSession.builder
              .appName("spark test")
              .master("local")
              .getOrCreate()
              
  spark.sparkContext.setLogLevel("ERROR")
  
  val rdd = spark.sparkContext.parallelize(1 to 10, 1)
  rdd.foreach(println)
//}
}