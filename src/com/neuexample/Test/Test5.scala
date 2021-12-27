package com.neuexample.Test

import java.lang.reflect.Field

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.entry.Alarm

object Test5 {


  def main(args: Array[String]): Unit = {

    var str="{\"maxTemperature\":18,\"driveMotorControllerTemperature\":false,\"dcTemperature\":false,\"controllerDcBusbarCurrent\":10000,\"batteryNumber\":1,\"temperatureDifferential\":false,\"year\":21,\"soc\":81,\"engineFailuresCount\":0,\"insulationResistance\":3965000,\"otherFailuresCodes\":[],\"maxVoltageSystemNum\":1,\"seconds\":37,\"driveMotorTemperature\":false,\"minVoltagebatteryNum\":2,\"deviceTypeOverVoltage\":false,\"engineFailuresCodes\":[],\"temperatureProbeCount\":16,\"vin\":\"my_test\",\"highPressureInterlock\":false,\"vehicleFactory\":\"1\",\"driveMotorCount\":1,\"day\":21,\"subsystemVoltageCount\":1,\"gears\":15,\"longitude\":4294967294,\"mileage\":235020,\"socHigh\":false,\"subsystemTemperatureCount\":1,\"level\":0,\"maxTemperatureNum\":9,\"minutes\":27,\"minTemperatureNum\":1,\"batteryCount\":96,\"insulation\":false,\"month\":12,\"deviceFailuresCount\":0,\"socJump\":false,\"subsystemTemperatureDataNum\":1,\"totalVoltage\":319648,\"vehicleStatus\":2,\"status\":254,\"deviceFailuresCodes\":[],\"driveMotorFailuresCodes\":[],\"maxVoltagebatteryNum\":60,\"latitude\":4294967294,\"torque\":65534,\"deviceTypeDontMatch\":false,\"socLow\":false,\"alarmInfo\":0,\"cellVoltages\":[3331,3329,3329,3329,3329,3329,3330,3329],\"chargeStatus\":3,\"speed\":65534,\"deviceTypeOverFilling\":false,\"controllerInputVoltage\":30,\"operationMode\":254,\"current\":10000,\"cellCount\":96,\"totalCurrent\":10000,\"minTemperature\":17,\"monomerBatteryUnderVoltage\":false,\"temperature\":17,\"deviceTypeUnderVoltage\":false,\"monomerBatteryOverVoltage\":false,\"batteryConsistencyPoor\":false,\"batteryMaxVoltage\":3332,\"batteryHighTemperature\":false,\"dcStatus\":false,\"hours\":7,\"riveMotorDataNum\":254,\"brakingSystem\":false,\"batteryMinVoltage\":3329,\"minTemperatureSystemNum\":1,\"driveMotorFailuresCount\":0,\"maxTemperatureSystemNum\":1,\"voltage\":319648,\"subsystemVoltageDataNum\":1,\"minVoltageSystemNum\":1,\"otherFailuresCount\":0,\"probeTemperatures\":[57,57,57,57,57,57,57,57,58,57,57,57,58,57,57,58],\"time\":1640051303431}";

    val json: JSONObject = JSON.parseObject(str)
    json.put("soc_high_value",1.999999);
    json.put("soc_jump_time",21321);

    val alarm: Alarm = JSON.toJavaObject(json,classOf[Alarm])

    println(alarm.soc_high_value)
    println(alarm.soc_jump_time)


    //val value: Class[Alarm2] = classOf[Alarm2]

    //val fields: Array[Field] = alarm.getClass.getDeclaredFields


//    println(alarm.vin)
//    println(alarm.probeTemperatures)
//    println(alarm.time)
//    println(alarm.current);
//    println(alarm.soc_high_value);
//    println(alarm.soc_high_time)
//
//    println(alarm.ctime)
//    println(alarm.start_time)
//
    //val fields: Array[Field] = classOf[Alarm3].getDeclaredFields

//    println(fields.length)
//    //fields2Scala(fields);
//    fields2Java(fields);

  }

  def fields2Java(fields:Array[Field]): Unit ={

    for( i <- 0 until fields.length ){

      val cur_type: String = fields(i).getType.toString
      // println(fields(i).getName+","+cur_type);
      if(cur_type.equals("class java.lang.String")){
        println( "String "+fields(i).getName+";"  );
      }else if(cur_type.equals("long")){
        println( "long "+fields(i).getName+";"  );
      }else if(cur_type.equals("int")){
        println( "int "+fields(i).getName+";"  );
      }else if(cur_type.equals("double")){
        println( "double "+fields(i).getName+";"  );
      }

    }

  }


  def fields2Scala(fields:Array[Field]): Unit ={
    for( i <- 0 until fields.length ){

      val cur_type: String = fields(i).getType.toString
      // println(fields(i).getName+","+cur_type);
      if(cur_type.equals("class java.lang.String")){
        println( "var "+fields(i).getName+":String = "+" \"\"  "  );
      }else if(cur_type.equals("long")){
        println( "var "+fields(i).getName+":Long = 0"  );
      }else if(cur_type.equals("int")){
        println( "var "+fields(i).getName+":Int = 0"  );
      }else if(cur_type.equals("double")){
        println( "var "+fields(i).getName+":Double = 0"  );
      }

    }
  }

}
