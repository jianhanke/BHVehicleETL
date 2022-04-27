package com.neuexample.streaming

import com.alibaba.fastjson.JSONObject
import com.neuexample.entry.AlarmEnum
import com.neuexample.utils.CommonFuncs.{mkctime, _}

object CommonVehicle {

  /**
    * 电压采集(电压采集线脱落）
    * @param json
    */
  def isVoltagelinefFall(json: JSONObject): Unit = {
    val cellVoltagesArray: Array[Int] = stringToIntArray(json.getString("cellVoltages"))
    if (cellVoltagesArray != null && cellVoltagesArray.filter(_!=0).length > 0) {//有正常值的情况
    val avgCellVoltage = cellVoltagesArray.filter(_!=0).sum / cellVoltagesArray.filter(_!=0).length
      if (avgCellVoltage >= 2000) {//过滤真实情况就是严重馈电的情况
        for (i <- 0 until cellVoltagesArray.length) {
          if (cellVoltagesArray(i) == 0) {//电压数组元素依次检索为0的情况
            json.put(AlarmEnum.voltageLineFall.toString, 2)
          }
        }
      }
    } else {//都为异常值的情况
      json.put(AlarmEnum.voltageLineFall.toString, 2)
    }
  }

  /**
    * 温度采集(温度采集线脱落)
    * @param json
    */
  def isTempLineFall(json: JSONObject): Unit = {
    val probeTemperaturesArray: Array[Int] = stringToIntArray(json.getString("probeTemperatures"))
    //    if(probeTemperaturesArray.contains(0)){
    //      println("==================")
    //      val ints = probeTemperaturesArray.filter(_ != 0)
    //      println(ints.mkString(","))
    //    }
    if (probeTemperaturesArray != null && probeTemperaturesArray.filter(_!=0).length > 0) {//有正常值的情况
    val avgTemperatures = (probeTemperaturesArray.filter(_!=0).sum / probeTemperaturesArray.filter(_!=0).length) - 40
      //      if(probeTemperaturesArray.contains(0)){
      //        println(avgTemperatures)
      //        println(probeTemperaturesArray.mkString(","))
      //      }
      if (avgTemperatures >= -15) {//过滤真实环境就是-40度左右的情况
        for (i <- 0 until probeTemperaturesArray.length) {
          if (probeTemperaturesArray(i) == 0) {//温度数组元素依次检索为0的情况
            json.put(AlarmEnum.tempLineFall.toString, 2)
          }
        }
      }
    } else {//都为异常值的情况
      json.put(AlarmEnum.tempLineFall.toString, 2)
    }
  }

  /**
    * 电压数据异常
    * @param json
    */
  def isVoltageAbnormal(json:JSONObject):Unit = {
    val cellVoltagesArray = stringToIntArray(json.getString("cellVoltages"))
    if (cellVoltagesArray != null && cellVoltagesArray.filter(_>=4800).nonEmpty) {//有大于4.8V的异常值
      if (cellVoltagesArray.filter(_<4800).length > 0) {//有正常值的情况
      val avgCellVoltages = cellVoltagesArray.filter(_<4800).sum / cellVoltagesArray.filter(_<4800).length
        if (avgCellVoltages <= 3600){//过滤真实环境过充的情况
          json.put(AlarmEnum.abnormalVoltage.toString, 2)
        }
      } else {//无正常值的情况
        json.put(AlarmEnum.abnormalVoltage.toString, 2)
      }
    }
  }

  /**
    * 温度数据异常
    * @param json
    */
  def isTempAbnormal(json:JSONObject):Unit = {
    val probeTemperaturesArray = stringToIntArray(json.getString("probeTemperatures"))
    if (probeTemperaturesArray != null && probeTemperaturesArray.filter(_>=165).length > 0) {//有大于125度的异常值
      if (probeTemperaturesArray.filter(_<165).length > 0) {//有正常值的情况
      val avgprobeTemperatures = probeTemperaturesArray.filter(_<165).sum / probeTemperaturesArray.filter(_<165).length
        if (avgprobeTemperatures <= 125) {//过滤真实环境处在高温的情况
          json.put(AlarmEnum.abnormalTemperature.toString, 2)
        }
      } else {//无正常值的情况
        json.put(AlarmEnum.abnormalTemperature.toString, 2)
      }
    }
  }

  /**
    *相邻单体采集异常
    * @param json
    */
  def isCellVoltageNeighborFault(json:JSONObject):Unit = {
    val cellVoltagesArray = stringToIntArray(json.getString("cellVoltages"))
    if (cellVoltagesArray != null) {
      val max = cellVoltagesArray.max
      val min = cellVoltagesArray.min
      val maxIndex = cellVoltagesArray.zipWithIndex.max._2 //最大电压所在序号
      val delta = math.abs(max - min)
      if (max >= 3000 && min <= 3400 && delta > 300) {//差值过滤，排除大电流带来的影响
        //除去最后一个单体为最高电压的情况
        if (maxIndex != (cellVoltagesArray.length - 1)) {
          if (math.abs(max - cellVoltagesArray(maxIndex + 1)) >= 0.95 * delta) {//0.95是兼容多组故障的影响
          val avg = Math.abs(max + cellVoltagesArray(maxIndex + 1)) /2
            if (cellVoltagesArray.filter(_!=max).filter(_!=cellVoltagesArray(maxIndex + 1)).nonEmpty) {
              val avgCell = cellVoltagesArray.filter(_!=max).filter(_!=cellVoltagesArray(maxIndex + 1)).sum/cellVoltagesArray.filter(_!=max).filter(_!=cellVoltagesArray(maxIndex + 1)).length//排除多组故障且最高电压相等的情况干扰
              if (math.abs(avg - avgCell) <= 100)//排除动态干扰
                json.put(AlarmEnum.isAdjacentMonomerAbnormal.toString, 2)
            }
          }
        }
        //除去第一个单体为最高电压的情况
        if (maxIndex != 0) {
          if (math.abs(max - cellVoltagesArray(maxIndex - 1)) >= 0.95 * delta) {
            val avg = Math.abs(max + cellVoltagesArray(maxIndex - 1))/2
            if (cellVoltagesArray.filter(_!=max).filter(_!=cellVoltagesArray(maxIndex - 1)).nonEmpty) {
              val avgCell = cellVoltagesArray.filter(_!=max).filter(_!=cellVoltagesArray(maxIndex - 1)).sum/cellVoltagesArray.filter(_!=max).filter(_!=cellVoltagesArray(maxIndex - 1)).length
              if (math.abs(avg - avgCell) <= 100)
                json.put(AlarmEnum.isAdjacentMonomerAbnormal.toString, 2)
            }
          }
        }
      }
    }
  }

}
