package com.neuexample.streaming


import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.utils.CommonFuncs._
import org.apache.spark.streaming.dstream.DStream

object SgmwStreaming {


  def addSgmwApi(persistsParts: DStream[String]): DStream[String]={
      addSgmwAlarm(persistsParts);
  }


  def addSgmwAlarm(persistsParts: DStream[String]): DStream[String]={
    val value: DStream[String] = persistsParts.map {

      line => {
        val json: JSONObject = JSON.parseObject(line)
         // println("comeing sgmw");

        isMonomerBatteryUnderVoltage(json);
        isMonomerBatteryOverVoltage(json);
        isDeviceTypeUnderVoltage(json);
        isDeviceTypeOverVoltage(json);

        isInsulationAlarm(json);

        isBatteryConsistencyPoor(json);

        isBatteryHighTemperature(json);

        isSocLow(json);

        json.toString
      }
    }
    value
  }


  def isInsulationAlarm( json:JSONObject){
    val insulationResistance: Integer = json.getInteger("insulationResistance")

    if(insulationResistance!=null  && insulationResistance > 0 ) {
      if (insulationResistance <= 40000) {
        json.put("insulation", 2);
      } else if (insulationResistance <= 100000) {
        json.put("insulation", 1);
      }
    }

  }

  def isMonomerBatteryUnderVoltage(json:JSONObject): Unit ={

    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    if(minCellVoltage!=null) {
      if (minCellVoltage <= 2200) {
        json.put("monomerBatteryUnderVoltage", 3);
      } else if (minCellVoltage <= 2400) {
        json.put("monomerBatteryUnderVoltage", 2);
      } else if (minCellVoltage <= 2500) {
        json.put("monomerBatteryUnderVoltage", 1);
      }
    }
  }

  //判断单体电池过压
  def isMonomerBatteryOverVoltage(json:JSONObject): Unit ={

    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    if(maxCellVoltage!=null) {
      if (maxCellVoltage >= 3750) {
        json.put("monomerBatteryOverVoltage",3);
      } else if (maxCellVoltage >= 3700) {
        json.put("monomerBatteryOverVoltage",2);
      } else if (maxCellVoltage >= 3680) {
        json.put("monomerBatteryOverVoltage",1);
      }
    }
  }

  //判断总电池欠压
  def isDeviceTypeUnderVoltage(json:JSONObject): Unit ={

    val totalVoltage: Integer = json.getInteger("totalVoltage")
    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    val cellCount: Integer = json.getInteger("cellCount")

    if(totalVoltage!=null  && cellCount!=null && minCellVoltage!=null ) {
      if (totalVoltage >= minCellVoltage * cellCount && totalVoltage <= 2000 * cellCount) {
        json.put("deviceTypeUnderVoltage",3);
      } else if (totalVoltage >= minCellVoltage * cellCount && totalVoltage <= 2300 * cellCount) {
        json.put("deviceTypeUnderVoltage",2);
      } else if (totalVoltage >= minCellVoltage * cellCount && totalVoltage <= 2500 * cellCount) {
        json.put("deviceTypeUnderVoltage",1);
      }
    }
  }

  //判断总电池过压
  def isDeviceTypeOverVoltage(json:JSONObject): Unit ={

    val totalVoltage: Integer = json.getInteger("totalVoltage")
    val cellCount: Integer = json.getInteger("cellCount")

    if(totalVoltage!=null  && cellCount!=null ) {
      if (totalVoltage >= 3750 * cellCount) {
        json.put("deviceTypeOverVoltage",3);
      } else if (totalVoltage >= 3700 * cellCount) {
        json.put("deviceTypeOverVoltage",2);
      } else if (totalVoltage >= 3660 * cellCount) {
        json.put("deviceTypeOverVoltage",1);
      }
    }

  }


  def isBatteryConsistencyPoor(json:JSONObject){

    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    if(maxCellVoltage!=null && minCellVoltage!=null){
      var diff=maxCellVoltage - minCellVoltage
      if( diff >= 1000 ){
        json.put("batteryConsistencyPoor", 3)
      }else if(diff >= 800){
        json.put("batteryConsistencyPoor", 2)
      }else if(diff >= 600){
        json.put("batteryConsistencyPoor", 1)
      }
    }

  }

  def isBatteryHighTemperature(json:JSONObject){


    val maxTemperature: Integer = json.getInteger("maxTemperature")

    if(maxTemperature != null) {
      if (maxTemperature >= 61  && maxTemperature <= 200 ) {
        json.put("batteryHighTemperature",3);
      } else if (maxTemperature >= 60   && maxTemperature <61  ) {
        json.put("batteryHighTemperature",2);
      } else if (maxTemperature >= 55  && maxTemperature < 60 ) {
        json.put("batteryHighTemperature",1);
      }
    }

  }

  def isSocLow(json:JSONObject){

    val soc: Integer = json.getInteger("soc")
    if(soc!=null && soc>0 &&  soc < 2){
      json.put("socLow",1);
    }

  }





}
