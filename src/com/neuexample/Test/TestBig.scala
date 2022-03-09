package com.neuexample.Test

import java.util
import java.util.Map

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.carrotsearch.sizeof.RamUsageEstimator

object TestBig {

  def main(args: Array[String]): Unit = {

    var str="{\"maxTemperature\":59}"
    var str2="{age:59}"

    val json: JSONObject = JSON.parseObject(str)

    val hanke: Integer = json.getIntValue("hanke")
    println("hanke:"+hanke)

    println(RamUsageEstimator.sizeOf(str))
    println(RamUsageEstimator.sizeOf(json))
    json.put("my_json",str2)

    println(json)

    val my_json: JSONObject = json.getJSONObject("my_json")
    println(my_json)
    println(my_json.getInteger("age"))

   var str3="{\"maxTemperature\":59,\"driveMotorControllerTemperature\":false,\"dcTemperature\":false,\"controllerDcBusbarCurrent\":10000,\"batteryNumber\":1,\"temperatureDifferential\":false,\"year\":22,\"soc\":60,\"engineFailuresCount\":0,\"insulationResistance\":40797000,\"otherFailuresCodes\":[],\"maxVoltageSystemNum\":1,\"seconds\":56,\"driveMotorTemperature\":false,\"minVoltagebatteryNum\":17,\"deviceTypeOverVoltage\":false,\"engineFailuresCodes\":[],\"temperatureProbeCount\":5,\"vin\":\"C33A018E972ACFC88\",\"highPressureInterlock\":false,\"vehicleFactory\":\"1\",\"driveMotorCount\":1,\"day\":19,\"subsystemVoltageCount\":1,\"gears\":16,\"longitude\":4294967294,\"mileage\":26214,\"socHigh\":false,\"subsystemTemperatureCount\":1,\"level\":0,\"maxTemperatureNum\":2,\"minutes\":47,\"minTemperatureNum\":1,\"batteryCount\":30,\"insulation\":false,\"month\":1,\"deviceFailuresCount\":0,\"socJump\":false,\"subsystemTemperatureDataNum\":1,\"totalVoltage\":991,\"vehicleStatus\":3,\"status\":254,\"deviceFailuresCodes\":[],\"driveMotorFailuresCodes\":[],\"maxVoltagebatteryNum\":16,\"latitude\":4294967294,\"torque\":65534,\"deviceTypeDontMatch\":false,\"socLow\":false,\"alarmInfo\":0,\"cellVoltages\":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],\"chargeStatus\":3,\"speed\":65534,\"deviceTypeOverFilling\":false,\"controllerInputVoltage\":710,\"operationMode\":254,\"current\":10046,\"cellCount\":30,\"totalCurrent\":10046,\"minTemperature\":57,\"monomerBatteryUnderVoltage\":false,\"temperature\":254,\"deviceTypeUnderVoltage\":false,\"monomerBatteryOverVoltage\":false,\"batteryConsistencyPoor\":false,\"batteryMaxVoltage\":3315,\"batteryHighTemperature\":false,\"dcStatus\":false,\"hours\":22,\"riveMotorDataNum\":254,\"brakingSystem\":false,\"batteryMinVoltage\":3299,\"minTemperatureSystemNum\":1,\"driveMotorFailuresCount\":0,\"maxTemperatureSystemNum\":1,\"voltage\":991,\"subsystemVoltageDataNum\":1,\"minVoltageSystemNum\":1,\"otherFailuresCount\":0,\"probeTemperatures\":[60,60,60,60,60],\"time\":1642603679302}";

    val json3: JSONObject = JSON.parseObject(str3)

    val keys: util.Set[String] = json3.keySet()

    val strings: util.Set[String] = json3.keySet()

    val keysSet: util.Set[String] = json3.keySet()









    val set: util.Set[Map.Entry[String, AnyRef]] = json3.entrySet()







    println(RamUsageEstimator.sizeOf(str))
    println(RamUsageEstimator.sizeOf(json))

  }

}
