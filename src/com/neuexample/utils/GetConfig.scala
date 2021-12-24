package com.neuexample.utils
import java.io.FileInputStream
import java.util.Properties
import org.apache.spark.sql.types._

object GetConfig {
  val gps_schema = StructType(List(StructField("province",StringType),StructField("city",StringType),StructField("area",StringType),StructField("lat",DoubleType),StructField("lon",DoubleType),StructField("region",StringType)))

  def getProperties(filename :String)={
    var properties = new Properties()
    properties.load(new FileInputStream(filename))
    properties
  }

  def main(args:Array[String]): Unit ={
    val properties = getProperties("F:/test.properties")
    println(properties.getProperty("mysql.conn"))
  }
}
