package com.neuexample.Test

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.utils.CommonFuncs.locateCityRDD
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.neuexample.utils.CommonFuncs._
import com.neuexample.utils.GetConfig

object StreamTest {

  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext
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
      "group.id" ->  properties.getProperty("kafka.consumer.groupid"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // 自动将偏移重置为最新的偏移，如果是第一次启动程序，应该为smallest，从头开始读
      "auto.offset.reset" -> properties.getProperty("kafka.auto.offset.reset")
    )



    // 用Kafka Direct API直接读数据
    val initStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )

    val df_gps_bc = ssc.sparkContext.broadcast(df_gps.collect())

    def parseCity(jsonstr:String) :String ={
      val jsonobject :JSONObject = JSON.parseObject(jsonstr)
      val lon = jsonobject.getDouble("longitude")
      val lat = jsonobject.getDouble("latitude")
      var locate = " , , , "
      if(lon != null && lat != null) {
        locate = locateCityRDD(lon / 1000000, lat / 1000000, df_gps_bc.value)
      }
      locate
    }

    val persistsParts: DStream[String] = initStream.map(_.value()).persist(StorageLevel.MEMORY_ONLY)

    // persistsParts.print()

    val geelyVehicldes: DStream[String] = persistsParts.filter(line => {
      val json: JSONObject = JSON.parseObject(line)
      val factory: String = json.getString("vehicleFactory")
      var isContainer=false;
      if( factory.equals("5") ){
        isContainer=true
        val strTemperatures: String = json.getString("probeTemperatures")
        if(strTemperatures.length >2 ) {
          val temperatureArray: Array[Int] = stringToIntArray(strTemperatures)
          if( temperatureArray.max - 40 == 87 ){
            isContainer=false
          }
        }
      }
      isContainer
    })
    geelyVehicldes.print()


    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)


  }
}
