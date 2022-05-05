package com.neuexample.streaming


import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.entry.AlarmEnum
import com.neuexample.utils.MathFuncs._
import com.neuexample.utils.CommonFuncs.{mkctime, _}
import org.apache.spark.streaming.{State, StateSpec}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import com.neuexample.streaming.CommonVehicle._

object Geely extends Serializable{




  def addGeelyApi(persistsParts: DStream[String]): DStream[String] = {

    println("geely come")

    addGeelyAlarm(persistsParts).mapWithState(StateSpec.function(func_state_geely))

  }

  val func_state_geely = (key: String, values: Option[JSONObject], state: State[JSONObject] ) => {


    val old_json: JSONObject = state.getOption().getOrElse(null)
    val json: JSONObject = values.get


    if(old_json != null){
      isSocHigh(old_json, json);
      isSocNotBalance(old_json, json);

      isSocJump(old_json, json);

      if (json.getIntValue(AlarmEnum.tempLineFall.toString) != 2 && json.getIntValue(AlarmEnum.abnormalTemperature.toString) != 2){
        isBatteryHighTemperature(old_json, json);
      }
      if (json.getIntValue(AlarmEnum.voltageLineFall.toString) != 2 && json.getIntValue(AlarmEnum.abnormalVoltage.toString) != 2 && json.getIntValue(AlarmEnum.isAdjacentMonomerAbnormal.toString) != 2){
        isElectricBoxWithWater(old_json, json);
      }

      isAbnormalCollect(old_json, json);
      isInsulation(old_json, json);
    }
    state.update(json);
    json.toString;
  }


  def addGeelyAlarm(persistsParts: DStream[String]): DStream[(String, JSONObject)] = {

     persistsParts.map {

      line => {

        val json: JSONObject = JSON.parseObject(line)

        json.put("ctime",
          mkctime(json.getInteger("year"), json.getInteger("month"), json.getInteger("day"), json.getInteger("hours"), json.getInteger("minutes"), json.getInteger("seconds")));

        //采集类故障先判定
        isTempLineFall(json)//温感采集线脱落报警
        isTempLineFall(json)//温感采集线脱落报警
        isTempAbnormal(json)//温度异常
        isVoltagelinefFall(json)//电压采集线脱落报警
        isVoltageAbnormal(json) //电压异常
        isCellVoltageNeighborFault(json) //相邻单体数据采集异常

        if (json.getIntValue(AlarmEnum.voltageLineFall.toString) != 2 && json.getIntValue(AlarmEnum.abnormalVoltage.toString) != 2 && json.getIntValue(AlarmEnum.isAdjacentMonomerAbnormal.toString) != 2){
          isMonomerBatteryUnderVoltage(json);
          isMonomerBatteryOverVoltage(json);
          isDeviceTypeUnderVoltage(json);
          isDeviceTypeOverVoltage(json);

          isBatteryConsistencyPoor(json);

          isOutFactorySafetyInspection(json);
        }

       //  isAbnormalTemperature(json);
       //  isAbnormalVoltage(json);

        (json.getString("vin"), json);
      }
    }


  }


  def isAbnormalVoltage(json: JSONObject): Unit = {

    val cellVoltageArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    val batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val batteryMinVoltage: Integer = json.getInteger("batteryMinVoltage")
    if(cellVoltageArray != null && batteryMaxVoltage != null && batteryMinVoltage != null && cellVoltageArray.length > 1 ){

      val quartile: (Double, Double) = calculateQuartile(cellVoltageArray)
      val  IQR: Double = quartile._2 - quartile._1
      var upperBoundary = quartile._2 + 1.5 * IQR;
      val downBoundary: Double = quartile._1 - 1.5 * IQR

      if( batteryMaxVoltage > upperBoundary || batteryMaxVoltage < downBoundary ||  batteryMinVoltage > upperBoundary || batteryMinVoltage < downBoundary   ){
          json.put("abnormalVoltageData", 1);
          json.put("voltage_uppder_boundary", upperBoundary)
          json.put("voltage_down_boundary", downBoundary)
      }
    }

  }

  def isAbnormalTemperature(json: JSONObject): Unit = {

    val probeTeptureArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    val maxTemperature: Integer = json.getInteger("maxTemperature")
    val minTemperature: Integer = json.getInteger("minTemperature")
    if(probeTeptureArray != null && maxTemperature != null && minTemperature != null && probeTeptureArray.length > 1 ){
      for( i <-  0 until probeTeptureArray.length){
          probeTeptureArray(i) = probeTeptureArray(i) - 40;
      }
      val quartile: (Double, Double) = calculateQuartile(probeTeptureArray)
      val  IQR: Double = quartile._2 - quartile._1
      var upperBoundary = quartile._2 + 1.5 * IQR;
      val downBoundary: Double = quartile._1 - 1.5 * IQR
      if( maxTemperature > upperBoundary || maxTemperature < downBoundary ||  minTemperature > upperBoundary || minTemperature < downBoundary   ){
        json.put("abnormalTemperature", 1);
        json.put("temperature_uppder_boundary", upperBoundary);
        json.put("temperature_down_boundary", downBoundary);
      }
    }
  }

  def isSocJump(old_json: JSONObject, json: JSONObject): Unit = {
    val ctime: Integer = json.getInteger("ctime")
    val old_ctime: Integer = old_json.getInteger("ctime")
    val soc: Integer = json.getInteger("soc")
    val old_soc: Integer = old_json.getInteger("soc")
    val current: Integer = json.getInteger("current")

    if(soc != null && old_soc != null  && current != null  && math.abs(soc-old_soc) >= 8 && math.abs(current) > 30000 && ctime > old_ctime && ctime-old_ctime < 30     ){
      json.put("socJump", 2);
      json.put("soc_jump_value", math.abs(soc - old_soc))
      json.put("current", math.abs(current));
      json.put("soc_jump_time", ctime - old_ctime)
      json.put("current", current);
      json.put("last_start_time", old_ctime)
    }
  }

  def isAbnormalCollect(old_json: JSONObject, json: JSONObject)  {

    var cycleCount: Integer = old_json.getIntValue("cycleCount") + 1;

    var batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage");
    if(batteryMaxVoltage != null &&  batteryMaxVoltage >= 65534){

        var abnormalTemperatureCount = old_json.getIntValue("abnormalTemperatureCount") + 1;

        if(abnormalTemperatureCount == 8){
          json.put("abnormalCollect", 2);
        }else if(abnormalTemperatureCount == 5){
          json.put("abnormalCollect", 1);
        }
        json.put("abnormalTemperatureCount", abnormalTemperatureCount);
    }

    if(cycleCount < 20){
      json.put("cycleCount", cycleCount);
    }else{
      json.remove("cycleCount")
      json.remove("abnormalTemperatureCount")
    }
  }


  def isBatteryHighTemperature(old_json: JSONObject, json: JSONObject): Unit = {

    val insulationResistance: Integer = json.getInteger("insulationResistance")
    val maxTemperature: Integer = json.getInteger("maxTemperature")
    val old_maxTemperature: Integer = old_json.getInteger("maxTemperature")
    val batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val batteryMinVoltage: Integer = json.getInteger("batteryMinVoltage")
    val totalVoltage: Integer = json.getInteger("totalVoltage")
    val old_ctime: Integer = old_json.getInteger("ctime")

    if(batteryMaxVoltage != null && batteryMinVoltage != null && totalVoltage != null  && batteryMaxVoltage - batteryMinVoltage >= 400
      && insulationResistance != null && insulationResistance / (totalVoltage / 1000.0) <= 500 && maxTemperature != null && old_maxTemperature != null )
      {
        val temperatureDiff: Integer = math.abs(maxTemperature-old_maxTemperature)
        if(  temperatureDiff > 30 || maxTemperature == 87   ){     //删除87度
          json.put("temperature_diff", temperatureDiff)
          json.put("batteryHighTemperature", 3);
          json.put("last_start_time", old_ctime)
        }else if(  temperatureDiff > 20 && temperatureDiff <= 30   ){
          json.put("temperature_diff", temperatureDiff)
          json.put("batteryHighTemperature", 2);
          json.put("last_start_time", old_ctime)
        }else if(  temperatureDiff >= 15 && temperatureDiff <= 20     ){
          json.put("temperature_diff", temperatureDiff)
          json.put("batteryHighTemperature", 1);
          json.put("last_start_time", old_ctime)
        }
      }

  }

  def isElectricBoxWithWater(old_json: JSONObject, json: JSONObject): Unit = {
    val ctime: Integer = json.getInteger("ctime")
    val old_ctime: Integer = old_json.getInteger("ctime")
    val secondsDiff: Integer = ctime - old_ctime

    val insulationResistance: Integer = json.getInteger("insulationResistance")
    val totalVoltage: Integer = json.getInteger("totalVoltage")
    val old_totalVoltage: Integer = old_json.getInteger("totalVoltage")


    if(totalVoltage != null && totalVoltage >= 100000 && totalVoltage <= 500000
      && old_totalVoltage != null && old_totalVoltage >= 100000 && old_totalVoltage <= 500000
      && insulationResistance != null && insulationResistance / ( totalVoltage / 1000.0) < 500
      && secondsDiff > 0 &&  secondsDiff <= 15
      &&  (totalVoltage - old_totalVoltage) / 1000.0 / secondsDiff > 0.05 )
    {
      json.put("electricBoxWithWater", 1);
      json.put("total_voltage_drop_rate", (totalVoltage - old_totalVoltage) / 1000.0 /secondsDiff);
      json.put("last_start_time", old_ctime)
    }


  }

  def isSocHigh(old_json: JSONObject, json: JSONObject) {

    val ctime: Integer = json.getInteger("ctime")
    val old_ctime: Integer = old_json.getInteger("ctime")
    val soc: Integer = json.getInteger("soc")
    val maxTemperature: Integer = json.getInteger("maxTemperature")
    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val current: Integer = json.getInteger("current")

    if(ctime != null && old_ctime != null && soc != null && maxTemperature != null && maxCellVoltage != null && current != null && current == 0  && soc <= 40 && ctime - old_ctime >= 3600 && ctime - old_ctime <= 86400    ) {
      val socMax: Double = calculateSoc(maxTemperature, maxCellVoltage )
      val timeDiff: Int = ctime - old_ctime
      val socSelfDis: Double = timeDiff / 1800 * 0.25

      if(soc - socMax - socSelfDis >= 10){
        json.put("socHigh", 2);
        json.put("soc_high_value", soc - socMax - socSelfDis);
        json.put("soc_high_time", timeDiff)
        json.put("last_start_time", old_ctime)
      }else if ( soc - socMax >= 10) {
        json.put("socHigh", 1);
        json.put("soc_high_value", soc - socMax);
        json.put("soc_high_time", timeDiff)
        json.put("last_start_time", old_ctime)
      }
    }
  }

  def isSocNotBalance(old_json: JSONObject, json: JSONObject) {

    val ctime: Integer = json.getInteger("ctime")
    val old_ctime: Integer = old_json.getInteger("ctime")

    val avgTemperature: Integer = json.getInteger("temperature")
    val current: Integer = json.getInteger("current")
    val batteryMaxVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val batteryMinVoltage: Integer = json.getInteger("batteryMinVoltage")

    if(ctime != null && old_ctime != null  && current != null && current == 0 && batteryMaxVoltage != null && batteryMinVoltage != null && ctime - old_ctime >= 3600 && ctime - old_ctime <= 86400) {
      val socMax: Double = calculateSoc(avgTemperature, batteryMaxVoltage)
      val socMin: Double = calculateSoc(avgTemperature, batteryMinVoltage)

      if(socMax - socMin >= 10){
        json.put("socNotBalance", 1);
        json.put("soc_diff_value", socMax - socMin);
        json.put("soc_notbalance_time", ctime - old_ctime);
        json.put("last_start_time", old_ctime);
      }
    }
  }


  def isVoltageJump(json: JSONObject){

    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    if(maxCellVoltage!=null && minCellVoltage!=null && minCellVoltage < 2000){
      json.put("voltageJump", 1);
    }

  }


  def isInsulation(old_json: JSONObject, json: JSONObject){
    val totalVoltage: Integer = json.getInteger("totalVoltage")
    val insulationResistance: Integer = json.getInteger("insulationResistance")
    var insulationCount: Int = old_json.getIntValue("insulationCount")
    var level = 0;

    json.put("insulation", false);

    if(insulationResistance != null && totalVoltage != null && insulationResistance > 0  ){
      if(totalVoltage <= 1000000) {
        if (insulationResistance / (totalVoltage / 1000.0) < 100) {
          level = 3;
          insulationCount += 1;
        } else if (insulationResistance / (totalVoltage / 1000.0) < 500) {
          level = 2;
          insulationCount += 1;
        }else{
          insulationCount -= 1;
        }
      }else{
        if(insulationResistance < 35000 ){
          insulationCount += 1;
          level = 3;
        }else if(insulationResistance < 175000 ){
          insulationCount += 1;
          level = 2;
        }else{
          insulationCount -= 1;
        }
      }
    }else{
      insulationCount -= 1;
    }
    if(insulationCount == 2){
      json.put("insulation", level);
    }
    json.put("insulationCount", insulationCount % 2);

  }

  // 判断单体电压欠压
  def isMonomerBatteryUnderVoltage(json: JSONObject){

    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
     val avgTemperature: lang.Double = json.getDouble("temperature")

    if(minCellVoltage != null && avgTemperature != null  && minCellVoltage <= 4700 ) {
      if (avgTemperature > 0) {               //判断单题电池欠压
        if (minCellVoltage < 2000) {
          json.put("monomerBatteryUnderVoltage", 3);
        } else if (minCellVoltage < 2450) {
          json.put("monomerBatteryUnderVoltage", 2);
        } else if (minCellVoltage < 2600) {
          json.put("monomerBatteryUnderVoltage", 1);
        }
      } else { //当温度 <=0
        if (minCellVoltage < 2000) {
          json.put("monomerBatteryUnderVoltage", 3);
        } else if (minCellVoltage < 2250) {
          json.put("monomerBatteryUnderVoltage", 2);
        } else if (minCellVoltage < 2400 ) {
          json.put("monomerBatteryUnderVoltage", 1);
        }
      }

    }

  }

  //判断单体电池过压
  def isMonomerBatteryOverVoltage(json: JSONObject){

    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")

    if (maxCellVoltage != null  &&   maxCellVoltage <= 4700) {
        if (maxCellVoltage > 4240) { //判断单体电池过压
          json.put("monomerBatteryOverVoltage", 3);
        } else if (maxCellVoltage > 4230) {
          json.put("monomerBatteryOverVoltage", 2);
        } else if (maxCellVoltage > 4220) {
          json.put("monomerBatteryOverVoltage", 1);
        }
    }

  }

  def isDeviceTypeUnderVoltage(json: JSONObject){

    val totalVoltage: Integer = json.getInteger("totalVoltage")
    val avgTemperature: lang.Double = json.getDouble("temperature")
    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    val cellCount: Integer = json.getInteger("cellCount")

    if(totalVoltage != null && avgTemperature != null && cellCount != null && minCellVoltage != null ){ //筛选总体电压，
      val minVoltage_cellCount: Int = minCellVoltage * cellCount

      if (avgTemperature > 0) {            //判断总电池欠压
        if(totalVoltage < 204000 && totalVoltage >= minVoltage_cellCount ){
          json.put("deviceTypeUnderVoltage", 3);
        }else if(totalVoltage < 249900  && totalVoltage >= minVoltage_cellCount  ){
          json.put("deviceTypeUnderVoltage", 2);
        }else if(totalVoltage < 265200  && totalVoltage >= minVoltage_cellCount ){
          json.put("deviceTypeUnderVoltage", 1);
        }
      }else{     //当温度 <=0
        if(totalVoltage < 204000  && totalVoltage >= minVoltage_cellCount  ){
          json.put("deviceTypeUnderVoltage", 3);
        }else if(totalVoltage < 229500 && totalVoltage >= minVoltage_cellCount ){
          json.put("deviceTypeUnderVoltage", 2);
        }else if(totalVoltage < 244800 && totalVoltage >= minVoltage_cellCount ){
          json.put("deviceTypeUnderVoltage", 1);
        }
      }

    }

  }

  def isDeviceTypeOverVoltage(json: JSONObject){

    val totalVoltage: Integer = json.getInteger("totalVoltage")
    if(totalVoltage != null && totalVoltage <= 500000){ //筛选总体电压，
      if(totalVoltage >  432500 ){      //判断总电池过压
        json.put("deviceTypeOverVoltage", 3);
      }else if( totalVoltage > 431500 ){
        json.put("deviceTypeOverVoltage", 2);
      }else if( totalVoltage > 430400 ){
        json.put("deviceTypeOverVoltage", 1);
      }
    }
  }





  def isVoltageJump(cellVoltageArray: Array[Int]): Boolean = {

    if(cellVoltageArray.max != 0 && cellVoltageArray.min != 0){
      if( cellVoltageArray.min < 2000 ){
        return true
      }
    }
    return false
  }


  def isBatteryConsistencyPoor(json: JSONObject){

    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    val current: Integer = json.getInteger("current")

    if(maxCellVoltage != null && minCellVoltage != null && current != null) {
      var diff = maxCellVoltage - minCellVoltage;
      if(diff > 600){
        json.put("batteryConsistencyPoor", 3);
      }else if(diff > 500){
        json.put("batteryConsistencyPoor", 2);
      }else if(diff > 400){
        json.put("batteryConsistencyPoor",1);
      }
    }

  }

  /*
      把二级报警去掉
   */

  def isOutFactorySafetyInspection(json: JSONObject){

    val maxCellVoltage: Integer = json.getInteger("batteryMaxVoltage")
    val minCellVoltage: Integer = json.getInteger("batteryMinVoltage")
    val current: Integer = json.getInteger("current")

    if(maxCellVoltage != null && minCellVoltage != null && current != null) {

      var diff = maxCellVoltage - minCellVoltage;
      if (current >= -2000 && current <= 2000) {
        if(diff > 60  ){
          json.put("outFactorySafetyInspection", 2);
        }/* else if( diff >30  ){
          json.put("outFactorySafetyInspection", 1);
        } */
      } else {
        if(diff > 90 ){
          json.put("outFactorySafetyInspection", 2);
        }/* else if( diff >60  ){
          json.put("outFactorySafetyInspection", 1);
        }
        */
      }

    }



  }

}
