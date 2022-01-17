package com.neuexample.etl

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Test2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.createDataFrame(
      Seq(
        (  "1", 1, "fs", 1, 1)
      )
    ).toDF( "vin", "start_time", "alarm_type", "end_time", "vehicle_factory")





    var properties = new Properties()
    properties.setProperty("user","battery")
    properties.setProperty("password","Abcd.123")

   // df.write.mode(SaveMode.Append).jdbc( properties.getProperty("mysql.conn"),"app_alarm_offline",properties )

    //df.show()

    val i: Double = -1 / 37000
    println( i )
    println( -1 / -37000 )


  }

  def uuid(): String ={
    "uuid()"
  }

}
