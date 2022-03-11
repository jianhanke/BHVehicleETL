package com.neuexample.Test

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.ArrayBuffer

object MyAlarm {

  def main(args: Array[String]): Unit = {

    var str="{\"maxTemperature\":59,\"driveMotorControllerTemperature\":false,\"dcTemperature\":false,\"controllerDcBusbarCurrent\":10000,\"batteryNumber\":1,\"temperatureDifferential\":false,\"year\":22,\"soc\":60,\"engineFailuresCount\":0,\"insulationResistance\":40797000,\"otherFailuresCodes\":[],\"maxVoltageSystemNum\":1,\"seconds\":56,\"driveMotorTemperature\":false,\"minVoltagebatteryNum\":17,\"deviceTypeOverVoltage\":false,\"engineFailuresCodes\":[],\"temperatureProbeCount\":5,\"vin\":\"C33A018E972ACFC88\",\"highPressureInterlock\":false,\"vehicleFactory\":\"1\",\"driveMotorCount\":1,\"day\":19,\"subsystemVoltageCount\":1,\"gears\":16,\"longitude\":4294967294,\"mileage\":26214,\"socHigh\":3,\"subsystemTemperatureCount\":1,\"level\":0,\"maxTemperatureNum\":2,\"minutes\":47,\"minTemperatureNum\":1,\"batteryCount\":30,\"insulation\":false,\"month\":1,\"deviceFailuresCount\":0,\"socJump\":false,\"subsystemTemperatureDataNum\":1,\"totalVoltage\":991,\"vehicleStatus\":3,\"status\":254,\"deviceFailuresCodes\":[],\"driveMotorFailuresCodes\":[],\"maxVoltagebatteryNum\":16,\"latitude\":4294967294,\"torque\":65534,\"deviceTypeDontMatch\":false,\"socLow\":true,\"alarmInfo\":0,\"cellVoltages\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],\"chargeStatus\":3,\"speed\":65534,\"deviceTypeOverFilling\":false,\"controllerInputVoltage\":710,\"operationMode\":254,\"current\":10046,\"cellCount\":30,\"totalCurrent\":10046,\"minTemperature\":57,\"monomerBatteryUnderVoltage\":false,\"temperature\":254,\"deviceTypeUnderVoltage\":false,\"monomerBatteryOverVoltage\":false,\"batteryConsistencyPoor\":false,\"batteryMaxVoltage\":3315,\"batteryHighTemperature\":false,\"dcStatus\":false,\"hours\":22,\"riveMotorDataNum\":254,\"brakingSystem\":false,\"batteryMinVoltage\":3299,\"minTemperatureSystemNum\":1,\"driveMotorFailuresCount\":0,\"maxTemperatureSystemNum\":1,\"voltage\":991,\"subsystemVoltageDataNum\":1,\"minVoltageSystemNum\":1,\"otherFailuresCount\":0,\"probeTemperatures\":[60,60,60,60,60],\"time\":1642603679302}"

    str="{\"hanke\":2}"
    val json: JSONObject = JSON.parseObject(str)

    println(json.getBoolean("hanke"))
    println(json.getBooleanValue("hanke"))

    val alarms = "batteryHighTemperature,socJump,socHigh,monomerBatteryUnderVoltage,monomerBatteryOverVoltage,deviceTypeUnderVoltage,deviceTypeOverVoltage,batteryConsistencyPoor,insulation,socLow,temperatureDifferential,voltageJump,socNotBalance,electricBoxWithWater,outFactorySafetyInspection,abnormalTemperature,abnormalVoltage,abnormalCollect"

    val alarm_types: Set[String] = alarms.split(",").toSet

    val arr = Array(1,2,3)

    val array: ArrayBuffer[(String,Integer)] = scala.collection.mutable.ArrayBuffer[(String,Integer)]()
    println(array.isEmpty)

    for(alarm_type <- alarm_types){
        var alarmValue: Integer = json.getInteger(alarm_type)
        if(alarmValue != null  && alarmValue > 0  ){
            if(alarmValue > 3){
              alarmValue =  1
            }
        }
    }
    for(i <- getArray){
      println("jianlail")
      println(i)
    }



  }
  def getArray: Array[Integer] ={
    return Array[Integer]()
  }

}
