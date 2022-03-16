package com.neuexample.streaming

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.entry.Alarm
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}

import scala.collection.mutable.ArrayBuffer
import com.neuexample.utils.CommonFuncs._
import org.apache.spark.storage.StorageLevel
import com.neuexample.streaming.Geely._
import com.neuexample.streaming.Sgmw._
import com.neuexample.utils.GetConfig
import com.neuexample.utils.GetConfig._
import org.apache.spark.broadcast.Broadcast


object WarningSteaming  extends Serializable{

//   PropertyConfigurator.configure("log4j.properties")

  def  main(args: Array[String]) {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()

     spark.sparkContext.setLogLevel("ERROR")

    val ssc =  new StreamingContext(spark.sparkContext, batchDuration = Seconds(2))
    ssc.checkpoint(properties.getProperty("checkpoint.dir"));

    val df_gps = spark.sparkContext.textFile("gps.csv").cache()
    val bc_df_gps = ssc.sparkContext.broadcast(df_gps.collect())
    //alarm监控列表
    val all_alarms = "batteryHighTemperature,socJump,socHigh,monomerBatteryUnderVoltage,monomerBatteryOverVoltage,deviceTypeUnderVoltage,deviceTypeOverVoltage,batteryConsistencyPoor,insulation,socLow,temperatureDifferential,voltageJump,socNotBalance,electricBoxWithWater,outFactorySafetyInspection,abnormalTemperature,abnormalVoltage,abnormalCollect"
    val bc_all_alarms: Broadcast[Set[String]] = ssc.sparkContext.broadcast(all_alarms.split(",").toSet)

    // Kafka配置参数
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> properties.getProperty("kafka.bootstrap.servers"),
      "group.id" ->  properties.getProperty("kafka.consumer.groupid"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // 自动将偏移重置为最新的偏移，如果是第一次启动程序，应该为smallest，从头开始读
      "auto.offset.reset" -> properties.getProperty("kafka.auto.offset.reset"),
      "enable.auto.commit" -> "true"
    )


    def parseCity(lon: lang.Double, lat: lang.Double): String ={
      var locate = " , , , "
      if(lon != null && lat != null) {
        locate = locateCityRDD(lon / 1000000, lat / 1000000, bc_df_gps.value)
      }
      locate
    }

    def getAlarms(json: JSONObject): ArrayBuffer[(String, Integer)] ={

      val array = scala.collection.mutable.ArrayBuffer[(String, Integer)]()
      val vehicleFactory: Integer = json.getInteger("vehicleFactory")
      var level = json.getIntValue("level")

      if(vehicleFactory == 1 || vehicleFactory == 5){      // 自定义算法
        for(alarm_type <- bc_all_alarms.value){
          level = json.getIntValue(alarm_type)
          if(level != 0){
            array += Tuple2(alarm_type, level);
          }
        }
      }else{
        if(level < 1 || level > 3 ){
          level = 1
        }
        for(alarm_type <- bc_all_alarms.value){
          if(json.getBooleanValue(alarm_type)){
            array += Tuple2(alarm_type, level);
          }
        }
      }
      array
    }

    // 用Kafka Direct API直接读数据
    val initStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](properties.getProperty("kafka.topic").split(",").toSet, kafkaParams)
    )

    val persistsParts: DStream[String] = initStream.map(_.value()).persist(StorageLevel.MEMORY_ONLY)


    val geelyVehicldes: DStream[String] = persistsParts.filter( line => {
      val json: JSONObject = JSON.parseObject(line)
        json.getString("vehicleFactory").equals("5")
    })


    val sgmwVehicles: DStream[String] = persistsParts.filter(line => {
      JSON.parseObject(line).getIntValue("vehicleFactory") == 1
    })


    val geelyVehicles: DStream[String] = persistsParts.filter( line => {
      JSON.parseObject(line).getIntValue("vehicleFactory") == 5
    })


    val otherVehicles: DStream[String] = persistsParts.filter(line => {
      var vehicleFactory: Int = JSON.parseObject(line).getIntValue("vehicleFactory")
      !(vehicleFactory == 1 || vehicleFactory == 5 || vehicleFactory ==2 )
    })


    val sgmwData: DStream[String] =  addSgmwApi(sgmwVehicles)
    val geelyData: DStream[String] = addGeelyApi(geelyVehicles)

    val vehicleData: DStream[String] = otherVehicles.union(sgmwData).union(geelyData)
    // val vehicleData: DStream[String] = geelyData.union(sgmwData)

    val persistsData: DStream[String] = vehicleData.persist(StorageLevel.MEMORY_ONLY)


    val lines: DStream[((String, String), Alarm)] =  persistsData.mapPartitions(
      iterable =>{
          var alarms = new ArrayBuffer[((String, String), Alarm)]()
          iterable.foreach( line => {

            val json: JSONObject = JSON.parseObject(line)
            var all_alarms: ArrayBuffer[(String, Integer)] = getAlarms(json)

            if(! all_alarms.isEmpty){
              var alarmBean = parseAlarm(json, parseCity(json.getDouble("longitude"), json.getDouble("latitude")))
              for( (alarm_type, alarm_level) <- all_alarms ){
                  var clone: Alarm = alarmBean.clone()
                  clone.alarm_type = alarm_type
                  clone.level = alarm_level
                  alarms.append(((clone.vin, clone.alarm_type), clone))
              }
            }

          }
        )
        alarms.iterator
      })

    /**
      *
       * key    DStream的key数据类型
       * values DStream的value数据类型
       *  state  是StreamingContext中之前该key的状态值
       *  (key, alarm, 是否插入, 是否更新)无则插入，有则更新
       */

    val func_alarm_divide = (key: (String, String), values: Option[Alarm], state: State[Alarm]) =>{

      var last_alarm: Alarm = state.getOption().getOrElse(null)
      var cur_alarm: Alarm = values.get

      if(last_alarm==null){
        // 无需连续两次
        if( cur_alarm.alarm_type.equals("socHigh") || cur_alarm.alarm_type.equals("socNotBalance")   || cur_alarm.alarm_type.equals("insulation") ){  //单独判断，则永远无法判断。
          (cur_alarm, true);
        }else {
          state.update(cur_alarm);
          (cur_alarm, false);
        }
      }else {

        if(  math.abs(cur_alarm.start_time - last_alarm.start_time)  < 20 ) {

          if( cur_alarm.alarm_type.equals("abnormalTemperature") || cur_alarm.alarm_type.equals("abnormalVoltage") ){

            cur_alarm.level=cur_alarm.level + last_alarm.level;
            if(cur_alarm.level==5){
              state.remove()
              (cur_alarm, true);
            }else{
              state.update(cur_alarm);
              (cur_alarm, false);
            }
          }else{
            state.remove()
            cur_alarm.setLast_alarm_time(last_alarm.start_time)
            (cur_alarm, true);
          }
        }else{
          state.update(cur_alarm);
          (cur_alarm, false);
        }

      }

    }

     val ll: MapWithStateDStream[(String, String), Alarm, Alarm, (Alarm, Boolean)] = lines.mapWithState(StateSpec.function(func_alarm_divide))


    ll.foreachRDD(
      rdd =>{
        rdd.foreachPartition(
          partitions =>{
            val conn = getMysqlConn(properties)
            try {
              partitions.foreach(record => {
                //插入
                if(record._2 == true) {
                  val insert_sql = "insert into app_alarm_divide_ps(uuid,vin,start_time,alarm_type,end_time,city,province,area,region,level,vehicle_factory,chargeStatus,mileage,voltage,current,soc,dcStatus,insulationResistance,maxVoltageSystemNum,maxVoltagebatteryNum,batteryMaxVoltage ,minVoltageSystemNum,minVoltagebatteryNum,batteryMinVoltage,maxTemperatureSystemNum,maxTemperatureNum,maxTemperature,minTemperatureSystemNum,minTemperatureNum,minTemperature,temperatureProbeCount,probeTemperatures,cellCount,cellVoltages,total_voltage_drop_rate,max_temperature_heating_rate,soc_high_value,soc_diff_value,soc_jump_value,soc_jump_time,battery_standing_time,temperature_diff,insulation_om_v,voltage_uppder_boundary,voltage_down_boundary,temperature_uppder_boundary,temperature_down_boundary,soc_notbalance_time,soc_high_time,last_alarm_time,longitude,latitude,speed) values(uuid(),'%s',%s,'%s',%s,'%s','%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    .format(record._1.vin
                      , record._1.start_time
                      , record._1.alarm_type
                      , record._1.ctime
                      , record._1.city
                      , record._1.province
                      , record._1.area
                      , record._1.region
                      , record._1.level
                      , record._1.vehicleFactory,
                        record._1.chargeStatus,
                      record._1.mileage,
                      record._1.voltage,
                      record._1.current,
                      record._1.soc, record._1.dcStatus, record._1.insulationResistance,
                      record._1.maxVoltageSystemNum, record._1.maxVoltagebatteryNum,
                      record._1.batteryMaxVoltage ,
                      record._1.minVoltageSystemNum, record._1.minVoltagebatteryNum, record._1.batteryMinVoltage,
                      record._1.maxTemperatureSystemNum, record._1.maxTemperatureNum, record._1.maxTemperature,
                      record._1.minTemperatureSystemNum, record._1.minTemperatureNum, record._1.minTemperature,
                      record._1.temperatureProbeCount, record._1.probeTemperatures, record._1.cellCount, record._1.cellVoltages,
                      record._1.total_voltage_drop_rate,record._1.max_temperature_heating_rate,record._1.soc_high_value,
                      record._1.soc_diff_value,record._1.soc_jump_value,record._1.soc_jump_time,record._1.battery_standing_time,
                      record._1.temperature_diff,record._1.insulation_om_v,
                      record._1.voltage_uppder_boundary,record._1.voltage_down_boundary,
                      record._1.temperature_uppder_boundary,record._1.temperature_down_boundary,
                      record._1.soc_notbalance_time,record._1.soc_high_time,record._1.last_alarm_time,
                      record._1.longitude,record._1.latitude,record._1.speed
                    )
                   println(insert_sql);
                  conn.prepareStatement(insert_sql).executeUpdate()
                }

              })
            }catch {
              case ex: Exception => {
                System.err.println("Process one data error, but program will continue! ", ex)
              }
            }
            finally{
              conn.close()
            }

          }
        )
      }
    )




    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }


  def parseAlarm(json: JSONObject, cityStr: String): Alarm ={

    val vehicleFactory: Int = json.getInteger("vehicleFactory")

    if(vehicleFactory != 5){
      json.put("ctime", mkctime(json.getInteger("year")
        , json.getInteger("month")
        , json.getInteger("day")
        , json.getInteger("hours")
        , json.getInteger("minutes")
        , json.getInteger("seconds")));
    }

    // (i18n_内蒙古自治区,i18n_巴彦淖尔市,i18n_磴口县,north,1.8326636882091768E7)
    //  , , ,
    // (i18n_内蒙古自治区,i18n_巴彦淖尔市,i18n_磴口县,north,1.8326636882091768E7)
    val cityArray: Array[String] = cityStr.split(",")
    json.put("province", cityArray(0));
    json.put("city", cityArray(1));
    json.put("area", cityArray(2));
    json.put("region", cityArray(3));

    JSON.toJavaObject(json, classOf[Alarm])
  }



}