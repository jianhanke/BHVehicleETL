package com.neuexample.etl


import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.entry.{Alarm, OfflineAlarm, Vehicle}
import com.neuexample.utils.CommonFuncs.{locateCityRDD, udf_mkctime}
import com.neuexample.utils.GetConfig
import com.neuexample.utils.GetConfig.getMysqlConn
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType

object OfflineWarning {


  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder()
      .appName("StreamingVehicleTrip")
      .master(properties.getProperty("spark.master"))
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

   val df_gps: Array[String] = spark.sparkContext.textFile("gps.csv").cache().collect()
    val udf_parseCity = udf( (lon:Long,lat:Long)=>{
         locateCityRDD(lon / 1000000, lat / 1000000, df_gps)
    } )


    // val df: DataFrame = spark.sql("select count(*) from warehouse_gx.dwd_vehicle where yyyymmdd >= 20220111 and yyyymmdd < 20220112")
    //select * from source_gx.ods_vehicledata where yyyymmdd >= 20220109 and yyyymmdd < 20220110
    val sourceDS: Dataset[Vehicle] = spark.sql("select * from warehouse_gx.dwd_vehicle where yyyymmdd = 20220111")
      .withColumn("ctime",udf_mkctime(col("year"),col("month"),col("day"),col("hours"),col("minutes"),col("seconds")))
      .filter(  col("vehicleFactory") === 5  )
      .limit(1000)
      .as[Vehicle]



    // sourceDS.show(false)
    // sourceDS.printSchema()

       val result = isAbnormalInternalResistance(sourceDS)



    val value: DataFrame = result.withColumnRenamed("vehiclefactory", "vehicle_factory")
      .withColumn("start_time", col("ctime"))
      .withColumn("end_time", col("ctime"))
      .withColumn("probetemperatures",udf_list_toString(col("probetemperatures")))
      .withColumn("cellvoltages",udf_list_toString(col("cellvoltages")))
      .withColumn("locate", udf_parseCity(col("longitude"),col("latitude")) )
      .select( "vin", "start_time", "alarm_type", "level", "end_time","locate", "vehicle_factory", "chargeStatus", "mileage", "voltage", "current", "soc", "dcStatus", "insulationResistance", "maxVoltageSystemNum", "maxVoltagebatteryNum", "batteryMaxVoltage", "minVoltageSystemNum", "minVoltagebatteryNum", "batteryMinVoltage", "maxTemperatureSystemNum", "maxTemperatureNum", "maxTemperature", "minTemperatureSystemNum", "minTemperatureNum", "minTemperature", "temperatureProbeCount", "cellCount", "longitude", "latitude", "speed","probetemperatures","cellvoltages")


    value.printSchema()

    value.show(false)

    value
      .write
      .mode("append")
      .format("jdbc")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("url", properties.getProperty("mysql.conn"))
      .option("dbtable", properties.getProperty("mysql.offline.table")) //表名
      .option("user", properties.getProperty("mysql.user"))
      .option("password", properties.getProperty("mysql.passwd"))
      .option("batchsize", "50000")
      .option("truncate", "true")
      .save()




    spark.stop()
  }

  /*
        13:01
        13:02
        13:03
        13:04
        13:05
        13:06
        吉利离线报异常：
        (1)吉利的 离线处理昨天进来的所有数据，每隔一天跑一次
        （2）从判断那一刻开始 连续5个点电流都是负的或者0 并且每两个点之间<30s
        (3)   !!!!!!(-0.4<最高单体电压差值 / 电流差值 <-1.5 ) 从判断那一刻比较   报警

   */

  def isAbnormalInternalResistance(sourceDS: Dataset[Vehicle]): Dataset[Row]={

    val w0: WindowSpec = Window.partitionBy("vin" )

    val w1: WindowSpec = Window.partitionBy("vin" ).orderBy( col("ctime").desc)

    val w2: WindowSpec = Window.partitionBy("vin" ).orderBy( col("ctime").desc).rowsBetween(-4,Window.currentRow)

    sourceDS
      .withColumn("last_ctime", lag("ctime", 1) over w1)
      .withColumn("current_count", udf_current_count(col("current")))
      // .withColumn("last_current_count",lag("current_count",1) over w1)
      //.withColumn("total_current_count", udf_series_current( col("ctime"), col("last_ctime"),col("last_current_count") ) +col("current_count")   )
      .withColumn("sum_current_count", sum("current_count") over w2)
      .withColumn("last_current", lag("current", 1) over w1)
      .withColumn("last_batterymaxvoltage", lag("batterymaxvoltage", 1) over w1)
      .withColumn("level", udf_isAbnormalInternalResistance(col("sum_current_count"), col("current"), col("batterymaxvoltage"), col("last_current"), col("last_batterymaxvoltage")))
      //.select("vin","ctime","last_ctime","current","current_count","sum_current_count","last_current","last_batterymaxvoltage","abnormalInternalResistance")
      .drop("last_ctime")
      .drop("current_count")
      .drop("sum_current_count")
      .drop("last_current")
      .drop("last_batterymaxvoltage")
      .where("level !=0  ")
      .withColumn("alarm_type",lit("abnormalInternalResistance") )


  }

  val udf_list_toString=udf( (list:Seq[Long])=>{
    list.mkString("[", ",", "]")
  })

  val udf_parse_locate=udf( (list:Seq[String])=>{
    list(1)
  })


  val udf_isAbnormalInternalResistance = udf (  (sum_current_count:Long,current:Long,batterymaxvoltage:Long,last_current:Long,last_batterymaxvoltage:Long) =>{

    val current_diff: Long = current - last_current
    if(current_diff != 0){
      val value: Long = (batterymaxvoltage - last_batterymaxvoltage) / current_diff
      if(  sum_current_count == 5 &&   (value < -1.5 || value > -0.4)  ){
          1
      }else{
          0
      }
    }else{
          0
    }
  } )

  val udf_add_field=udf( (fileName:String)=>{
        fileName
  } )


  val udf_current_count=udf ( (current:Long)=>{
        if( current <= 0 ){
              1
        }else{
              0
        }
  }  )

  val udf_series_current = udf(  ( ctime:Long,last_ctime:Long,lead_current_count:Long)=>{



    if( last_ctime - ctime  <= 30 ){
        lead_current_count
    }else{
        0
    }
  } )



}
