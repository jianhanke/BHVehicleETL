package com.neuexample.streaming

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
import java.util.Properties

import com.neuexample.streaming.Geely._
import com.neuexample.streaming.Sgmw._
import com.neuexample.utils.GetConfig
import com.neuexample.utils.GetConfig._
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


object WarningSteaming  extends Serializable{

//   PropertyConfigurator.configure("log4j.properties")

  def  main(args: Array[String]) {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR");


    val ssc =  new StreamingContext(sc, batchDuration = Seconds(2))
    ssc.checkpoint(properties.getProperty("checkpoint.dir"));

    val df_gps = spark.sparkContext.textFile("gps.csv").cache()


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

    // 用Kafka Direct API直接读数据
    val initStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](properties.getProperty("kafka.topic").split(",").toSet, kafkaParams)
    )



    val df_gps_bc = ssc.sparkContext.broadcast(df_gps.collect())

    //alarm监控列表
    val alarms = "batteryHighTemperature,socJump,socHigh,monomerBatteryUnderVoltage,monomerBatteryOverVoltage,deviceTypeUnderVoltage,deviceTypeOverVoltage,batteryConsistencyPoor,insulation,socLow,temperatureDifferential,voltageJump,socNotBalance,electricBoxWithWater,outFactorySafetyInspection,abnormalTemperature,abnormalVoltage"

    val bc_alarmSet: Broadcast[Set[String]] = ssc.sparkContext.broadcast(alarms.split(",").toSet)

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


    val geelyVehicldes: DStream[String] = persistsParts.filter( line => {
      val json: JSONObject = JSON.parseObject(line)
        json.getString("vehicleFactory").equals("5")
    })


    val sgmwVehicldes: DStream[String] = persistsParts.filter(line => {
      val json: JSONObject = JSON.parseObject(line)
       json.getString("vehicleFactory").equals("1")
    })



     val geelyData: DStream[String] = addGeelyApi(geelyVehicldes)
     val sgmwData: DStream[String] =  addSgmwApi(sgmwVehicldes)


     // val vehicleData: DStream[String] = otherVehicldes.union(sgmwData).union(geelyData)
    val vehicleData: DStream[String] = geelyData.union(sgmwData)

    val persistsData: DStream[String] = vehicleData.persist(StorageLevel.MEMORY_ONLY)


    val lines :DStream[((String,String),Alarm)] =  persistsData.mapPartitions(
      iterable =>{
          var alarms = new ArrayBuffer[((String,String),Alarm)]()
          iterable.foreach( line => {

            for(alarm_column <- bc_alarmSet.value) {
              // println(alarm_column)
              val this_alarm = parseAlarm(line, alarm_column,parseCity(line))
             if (this_alarm.alarm_val != 0) {
              alarms.append(((this_alarm.vin, this_alarm.alarm_type), this_alarm))
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
       *  (key,alarm,是否插入,是否更新)无则插入，有则更新
       */

    val func_alarm_divide = (key :(String,String),values :Option[Alarm],state :State[Alarm]) =>{

      var last_alarm: Alarm = state.getOption().getOrElse(null)
      var cur_alarm :Alarm = values.get

      if(last_alarm==null){

        if( cur_alarm.alarm_type.equals("socHigh") || cur_alarm.alarm_type.equals("socNotBalance") ){  //socHigh不单独判断，则永远无法判断。
          (cur_alarm,true);
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
                  val insert_sql = "insert into app_alarm_divide_dwd(uuid,vin,start_time,alarm_type,end_time,city,province,area,region,level,vehicle_factory,chargeStatus,mileage,voltage,current,soc,dcStatus,insulationResistance,maxVoltageSystemNum,maxVoltagebatteryNum,batteryMaxVoltage ,minVoltageSystemNum,minVoltagebatteryNum,batteryMinVoltage,maxTemperatureSystemNum,maxTemperatureNum,maxTemperature,minTemperatureSystemNum,minTemperatureNum,minTemperature,temperatureProbeCount,probeTemperatures,cellCount,cellVoltages,total_voltage_drop_rate,max_temperature_heating_rate,soc_high_value,soc_diff_value,soc_jump_value,soc_jump_time,battery_standing_time,temperature_diff,insulation_om_v,voltage_uppder_boundary,voltage_down_boundary,temperature_uppder_boundary,temperature_down_boundary,soc_notbalance_time,soc_high_time,last_alarm_time,longitude,latitude,speed) values(uuid(),'%s',%s,'%s',%s,'%s','%s','%s','%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'%s',%s,'%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
                      record._1.voltage_uppder_boundary:Double,record._1.voltage_down_boundary:Double,
                      record._1.temperature_uppder_boundary:Double,record._1.temperature_down_boundary:Double,
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


  def parseAlarm(jsonstr:String,alarm_type :String,cityStr :String) :Alarm ={
    val json :JSONObject = JSON.parseObject(jsonstr)


    val vehicleFactory: Int = json.getInteger("vehicleFactory")
    var level = json.getInteger("level")

    if (vehicleFactory == 5 ||  vehicleFactory == 1  ) { // 只处理吉利车
      level=json.getInteger(alarm_type);
    }
    //告警等级
    if (level != null && level > 0 && level < 4 ){

    }else{
      level = 1
    }

//    if(vehicleFactory == 2  ){  // 暂时将 江淮发的警报置为空，后续如果单独添设计江淮警报，删除此行。
//          json.put(alarm_type,false);
//    }


    val ctime = mkctime(json.getInteger("year")
      ,json.getInteger("month")
      ,json.getInteger("day")
      ,json.getInteger("hours")
      ,json.getInteger("minutes")
      ,json.getInteger("seconds"))

    json.put("ctime",ctime);
    json.put("alarm_type",alarm_type);
    json.put("alarm_val",json.getIntValue(alarm_type))
    json.put("level",level);
    //var isSendAlarm:Int = if(jsonobject.containsKey(alarm_type)) jsonobject.getInteger(alarm_type) else 0;


    // (i18n_内蒙古自治区,i18n_巴彦淖尔市,i18n_磴口县,north,1.8326636882091768E7)
    val cityArray: Array[String] = cityStr.split(",")
    json.put("province",cityArray(0));
    json.put("city",cityArray(1));
    json.put("area",cityArray(2));
    json.put("region",cityArray(3));


    JSON.toJavaObject(json,classOf[Alarm])
  }



}