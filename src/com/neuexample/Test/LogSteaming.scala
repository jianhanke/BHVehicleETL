package com.neuexample.Test

import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.utils.GetConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

import scala.collection.mutable.ArrayBuffer


object LogSteaming  extends Serializable{


  //alarm监控列表
  val alarms = "batteryHighTemperature,socJump,socNotBalance,socHigh,monomerBatteryUnderVoltage,monomerBatteryOverVoltage,deviceTypeUnderVoltage,deviceTypeOverVoltage,batteryConsistencyPoor,insulation,socLow,temperatureDifferential,voltageJump,electricBoxWithWater,outFactorySafetyInspection,abnormalTemperature,abnormalVoltage"
  val alarmSet:Set[String] = alarms.split(",").toSet
  val properties = GetConfig.getProperties("test.properties")
//   PropertyConfigurator.configure("log4j.properties")

  //创建mysql连接
  def getMysqlConn(properties :Properties) :Connection={
    Class.forName("com.mysql.cj.jdbc.Driver")
    //获取mysql连接
    val conn: Connection = DriverManager.getConnection(properties.getProperty("mysql.conn"), properties.getProperty("mysql.user"), properties.getProperty("mysql.passwd"))
    conn
  }

  def  main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR");
    val ssc =  new StreamingContext(sc, batchDuration = Seconds(2))
    ssc.checkpoint(properties.getProperty("checkpoint.dir"));
    //gps Dataframe
    val df_gps = spark.sparkContext.textFile("gps.csv")
    df_gps.cache()
    // Kafka的topic
    val topics = properties.getProperty("kafka.topic")
    val topicsSet: Set[String] = topics.split(",").toSet
    // Kafka配置参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> properties.getProperty("kafka.bootstrap.servers"),
      "group.id" ->  "dwd_Test1",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // 自动将偏移重置为最新的偏移，如果是第一次启动程序，应该为smallest，从头开始读
      "auto.offset.reset" -> properties.getProperty("kafka.auto.offset.reset")
    )

    PropertyConfigurator.configure("log4j.properties")



    // 用Kafka Direct API直接读数据
    val initStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )

    val persistsParts: DStream[String] = initStream.map(_.value()).persist(StorageLevel.MEMORY_ONLY)

    val geelyVehicldes: DStream[String] = persistsParts.filter(line => {
      val json: JSONObject = JSON.parseObject(line)
      val factory: String = json.getString("vehicleFactory")
      factory.equals("5")
    })


    geelyVehicldes.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          partitions => {

            partitions.foreach { record => {

              val json: JSONObject = JSON.parseObject(record)
              val vin: String = json.getString("vin")

              if(vin.equals("LB3752CW9MAOSSCEO") || vin.equals("LB3752CW0MAOSCCGC")  || vin.equals("LB3752CW0MAOSCCTN") || vin.equals("LB3752CW1MAOSCNCH") || vin.equals("LB3752CW8MAOSSRSS") || vin.equals("LB3752CW1MAOSCNCH")
                  || vin.equals("LB3752CW1MAOSCNCH")
              ){
                println(json);
              }

            }

            }
          }
        )
      }
    )



    initStream.foreachRDD(rdd => {
      var offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      initStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }



}