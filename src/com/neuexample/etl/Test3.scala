package com.neuexample.etl

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.utils.CommonFuncs._
import com.neuexample.utils.GetConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test3 {


  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder()
      .appName("StreamingVehicleTrip")
      .master(properties.getProperty("spark.master"))
      .enableHiveSupport()
      .getOrCreate()




  }

}
