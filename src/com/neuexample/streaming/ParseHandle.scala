package com.neuexample.streaming

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.entry.Alarm
import com.neuexample.utils.CommonFuncs.{locateCityRDD, mkctime}
import org.apache.spark.streaming.dstream.DStream
import com.neuexample.streaming.WarningSteaming._

import scala.collection.mutable.ArrayBuffer

object ParseHandle {

  def parseAlarm(jsonstr:String,alarm_type :String,cityStr :String) :Alarm ={
    val jsonobject :JSONObject = JSON.parseObject(jsonstr)
    val vin = jsonobject.getString("vin")

    val vehicleFactory: Int = jsonobject.getInteger("vehicleFactory")
    var level = jsonobject.getInteger("level")

    if (vehicleFactory == 5 ||  vehicleFactory == 1  ) { // 只处理吉利车
      level=jsonobject.getInteger(alarm_type);
    }
    //告警等级
    if (level != null && level > 0 && level < 4 ){

    }else{
      level = 1
    }

    val ctime = mkctime(jsonobject.getInteger("year")
      ,jsonobject.getInteger("month")
      ,jsonobject.getInteger("day")
      ,jsonobject.getInteger("hours")
      ,jsonobject.getInteger("minutes")
      ,jsonobject.getInteger("seconds"))

    var isSendAlarm:Int = if(jsonobject.containsKey(alarm_type)) jsonobject.getInteger(alarm_type) else 0;
 

    val chargeStatus: Integer = if(jsonobject.containsKey("chargeStatus")) jsonobject.getInteger("chargeStatus") else 0;
    val mileage: Long = if(jsonobject.containsKey("mileage")) jsonobject.getLong("mileage") else 0;
    val voltage: Integer = if(jsonobject.containsKey("voltage")) jsonobject.getInteger("voltage") else 0;
    val current: Integer = if(jsonobject.containsKey("current")) jsonobject.getInteger("current") else 0;
    val soc: Integer = if(jsonobject.containsKey("soc")) jsonobject.getInteger("soc") else 0;
    val dcStatus: Integer = if(jsonobject.containsKey("dcStatus")) jsonobject.getInteger("dcStatus") else 0;
    val insulationResistance: Integer = if(jsonobject.containsKey("insulationResistance")) jsonobject.getInteger("insulationResistance") else 0;
    val maxVoltageSystemNum: Integer = if(jsonobject.containsKey("maxVoltageSystemNum")) jsonobject.getInteger("maxVoltageSystemNum") else 0;
    val maxVoltagebatteryNum: Integer = if(jsonobject.containsKey("maxVoltagebatteryNum")) jsonobject.getInteger("maxVoltagebatteryNum") else 0;
    val batteryMaxVoltage: Integer = if(jsonobject.containsKey("batteryMaxVoltage")) jsonobject.getInteger("batteryMaxVoltage") else 0;
    val minVoltageSystemNum: Integer = if(jsonobject.containsKey("minVoltageSystemNum")) jsonobject.getInteger("minVoltageSystemNum") else 0;
    val minVoltagebatteryNum: Integer = if(jsonobject.containsKey("minVoltagebatteryNum")) jsonobject.getInteger("minVoltagebatteryNum") else 0;
    val batteryMinVoltage: Integer = if(jsonobject.containsKey("batteryMinVoltage")) jsonobject.getInteger("batteryMinVoltage") else 0;
    val maxTemperatureSystemNum: Integer = if(jsonobject.containsKey("maxTemperatureSystemNum")) jsonobject.getInteger("maxTemperatureSystemNum") else 0;
    val maxTemperatureNum: Integer = if(jsonobject.containsKey("maxTemperatureNum")) jsonobject.getInteger("maxTemperatureNum") else 0;
    val maxTemperature: Integer = if(jsonobject.containsKey("maxTemperature")) jsonobject.getInteger("maxTemperature") else 0;
    val minTemperatureSystemNum: Integer = if(jsonobject.containsKey("minTemperatureSystemNum")) jsonobject.getInteger("minTemperatureSystemNum") else 0;
    val minTemperatureNum: Integer = if(jsonobject.containsKey("minTemperatureNum")) jsonobject.getInteger("minTemperatureNum") else 0;
    val minTemperature: Integer = if(jsonobject.containsKey("minTemperature")) jsonobject.getInteger("minTemperature") else 0;
    val temperatureProbeCount: Integer = if(jsonobject.containsKey("temperatureProbeCount")) jsonobject.getInteger("temperatureProbeCount") else 0;
    val probeTemperatures: String = if(jsonobject.containsKey("probeTemperatures")) jsonobject.getString("probeTemperatures") else "";
    val cellCount: Integer = if(jsonobject.containsKey("cellCount")) jsonobject.getInteger("cellCount") else 0;
    val cellVoltages: String = if(jsonobject.containsKey("cellVoltages")) jsonobject.getString("cellVoltages") else "";



    val total_voltage_drop_rate: Double = if(jsonobject.containsKey("total_voltage_drop_rate")) jsonobject.getDouble("total_voltage_drop_rate") else 0;
    val max_temperature_heating_rate: Double = if(jsonobject.containsKey("max_temperature_heating_rate")) jsonobject.getDouble("max_temperature_heating_rate") else 0;
    val soc_high_value: Double = if(jsonobject.containsKey("soc_high_value")) jsonobject.getDouble("soc_high_value") else 0;
    val soc_diff_value: Double = if(jsonobject.containsKey("soc_diff_value")) jsonobject.getDouble("soc_diff_value") else 0;
    val soc_jump_value: Double = if(jsonobject.containsKey("soc_jump_value")) jsonobject.getDouble("soc_jump_value") else 0;
    val soc_jump_time: Int = if(jsonobject.containsKey("soc_jump_time")) jsonobject.getInteger("soc_jump_time") else 0;
    val battery_standing_time: Int = if(jsonobject.containsKey("battery_standing_time")) jsonobject.getInteger("battery_standing_time") else 0;
    val temperature_diff: Int = if(jsonobject.containsKey("temperature_diff")) jsonobject.getInteger("temperature_diff") else 0;
    val insulation_om_v: Double = if(jsonobject.containsKey("insulation_om_v")) jsonobject.getDouble("insulation_om_v") else 0;
    val voltage_uppder_boundary: Double = if(jsonobject.containsKey("voltage_uppder_boundary")) jsonobject.getDouble("voltage_uppder_boundary") else 0;
    val voltage_down_boundary: Double = if(jsonobject.containsKey("voltage_down_boundary")) jsonobject.getDouble("voltage_down_boundary") else 0;
    val temperature_uppder_boundary: Double = if(jsonobject.containsKey("temperature_uppder_boundary")) jsonobject.getDouble("temperature_uppder_boundary") else 0;
    val temperature_down_boundary: Double = if(jsonobject.containsKey("temperature_down_boundary")) jsonobject.getDouble("temperature_down_boundary") else 0;
    val soc_notbalance_time: Int = if(jsonobject.containsKey("soc_notbalance_time")) jsonobject.getInteger("soc_notbalance_time") else 0;
    val soc_high_time: Int = if(jsonobject.containsKey("soc_high_time")) jsonobject.getInteger("soc_high_time") else 0;

    val cityArray: Array[String] = cityStr.split(",")

    val alarm = new Alarm(vin,ctime,alarm_type,isSendAlarm,cityArray(0),cityArray(1),cityArray(2),cityArray(3),level,vehicleFactory,
      chargeStatus,mileage,voltage,current,soc,dcStatus,insulationResistance,maxVoltageSystemNum,maxVoltagebatteryNum,
      batteryMaxVoltage,minVoltageSystemNum,minVoltagebatteryNum,batteryMinVoltage,maxTemperatureSystemNum,
      maxTemperatureNum,maxTemperature,minTemperatureSystemNum,minTemperatureNum,minTemperature,temperatureProbeCount,
      probeTemperatures,cellCount,cellVoltages,
      total_voltage_drop_rate,max_temperature_heating_rate,soc_high_value,soc_diff_value,soc_jump_value,soc_jump_time,battery_standing_time,
      temperature_diff,insulation_om_v,voltage_uppder_boundary,voltage_down_boundary,temperature_uppder_boundary,temperature_down_boundary,
      soc_notbalance_time,soc_high_time
    )

    alarm
  }




}
