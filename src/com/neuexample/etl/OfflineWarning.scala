package com.neuexample.etl

import com.neuexample.utils.CommonFuncs.udf_mkctime
import com.neuexample.utils.GetConfig
import com.neuexample.utils.GetConfig.getMysqlConn
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OfflineWarning {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StreamingVehicleTrip")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val properties = GetConfig.getProperties("test.properties")

    val s_yyyymmdd = "20220105"
    val e_yyyymmdd = "20220106"

    // val df: DataFrame = spark.sql("select count(*) from warehouse_gx.dwd_vehicle where yyyymmdd >= 20220111 and yyyymmdd < 20220112")
    //select * from source_gx.ods_vehicledata where yyyymmdd >= 20220109 and yyyymmdd < 20220110
    val df: DataFrame = spark.sql("select * from warehouse_gx.dwd_vehicle where yyyymmdd = 20220111")
      .filter(  col("vehicleFactory") === 1 )
      .withColumn("ctime",udf_mkctime(col("year"),col("month"),col("day"),col("hours"),col("minutes"),col("seconds")))
      .limit(1000);

    val w1: WindowSpec = Window.partitionBy("vin" ).orderBy("ctime")
    val w2: WindowSpec = Window.partitionBy("vin" ).orderBy("ctime").rowsBetween(Window.currentRow,4)


    val basicDF: DataFrame =

      df.withColumn("afterFiveCurrent", collect_list("current") over w2 )
      .withColumn("afterFiveCtime", collect_list("ctime") over w2 )
      .withColumn("afterOneCurrent", lead("current",1 ) over w1 )
      .withColumn("afterOneBatteryMaxVoltage", lead("batteryMaxVoltage",1 ) over w1 )
      .select("vin","ctime","batteryMaxVoltage","current","afterFiveCurrent","afterFiveCtime","afterOneCurrent","afterOneBatteryMaxVoltage","vehicleFactory")
        .cache()
    basicDF.show(false)
    basicDF.printSchema()







//    basicDF.foreachPartition(
//
//      partition=>{
//        val conn = getMysqlConn(properties)
//        try {
//          partition.foreach(line => {
//
//            if(!line.isNullAt(6)  && !line.isNullAt(7) && !line.isNullAt(3) ){
//              val batteryMaxVoltage: Long = line.getLong(2)
//              val current: Long = line.getLong(3)
//              val afterOneCurrent: Long = line.getLong(6)
//              val afterOneBatteryMaxVoltage: Long = line.getLong(7)
//
//              if(current-afterOneCurrent !=0) {
//
//                val diff: Long = (batteryMaxVoltage - afterOneBatteryMaxVoltage) / (current - afterOneCurrent)
//                if (diff < -1.5 || diff > -0.4) {
//
//                  val insert_sql = "insert into app_alarm_offline(uuid,vin,start_time,alarm_type,end_time,level,vehicle_factory) values(uuid(),'%s',%s,'%s',%s,%s,%s)"
//                    .format(
//                      line.getString(0),
//                      line.getLong(1),
//                      "abnormalInternalResistance",
//                      line.getLong(1),
//                      1,
//                      line.getString(8).toLong
//                    )
//                  //println(insert_sql)
//                 // conn.prepareStatement(insert_sql).executeUpdate()
//                } else {
//
//                }
//              }
//
//            }
//
//
//
//          })
//        }catch {
//          case ex: Exception => {
//            System.err.println("Process one data error, but program will continue! ", ex)
//          }
//        } finally{
//          conn.close()
//        }
//
//
//      }
//
//    )







  }


}
