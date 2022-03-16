package com.neuexample.utils
import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.types._

object GetConfig {
  val gps_schema = StructType(List(StructField("province",StringType),StructField("city",StringType),StructField("area",StringType),StructField("lat",DoubleType),StructField("lon",DoubleType),StructField("region",StringType)))

  def getProperties(filename: String)={
    var properties = new Properties()
    properties.load(new FileInputStream(filename))
    properties
  }

  //创建mysql连接
  def getMysqlConn(properties: Properties): Connection={
    Class.forName("com.mysql.cj.jdbc.Driver")
    //获取mysql连接
     DriverManager.getConnection(properties.getProperty("mysql.conn"), properties.getProperty("mysql.user"), properties.getProperty("mysql.passwd"))
  }

}
