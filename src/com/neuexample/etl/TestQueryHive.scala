package com.neuexample.etl


import com.neuexample.utils.GetConfig
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TestQueryHive {

  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder()
      .appName("StreamingVehicleTrip")
      .master(properties.getProperty("spark.master"))
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


     spark.sql("show databases").show(false)

    spark.sql("select count(*) from source_gx.ods_vehicledata where yyyymmdd = 20220111").show(false)

    spark.sql("select * from source_gx.ods_vehicledata where yyyymmdd = 20220111").show(false)

    spark.stop()
  }

}
