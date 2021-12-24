package com.neuexample.Test

import com.alibaba.fastjson.{JSON, JSONObject}

object Test {

  def main(args: Array[String]): Unit = {


    var strJson:String="{'name':'hanke' }";
    val json: JSONObject = JSON.parseObject(strJson)

    val age1:Int=json.getInteger("ages");
    val age2:Integer=json.getInteger("ages");

    println(age1)   //Exception in thread "main" java.lang.NullPointerException
    println(age2)  /// false

    println(age1==null)
    println(age2==null);






    println(json)


   // val beijing: Int = json.getInteger("beijing")

    var str2="curTime:1639663377,lastTime:1639663373,diff:4";
    val str: String = str2.substring(0,7)
    println(str)


    var arr=Array(10,1,25,11,24,13,54,4,42,-3);
     arr.sortWith(_ < _)
    println(arr.mkString(","))

  }


  def isInsulationAlarm( insulationResistance: Integer  ): Int ={

    /*
    val insulate: Double = insulationResistance / (totalVoltage/1000.0)
    var res=0;
    if(  insulate <= 25 ){   //100
        res=2
    }else if(  insulate <= 50 ){  //500
        res=1;
    }
    res
    */
    var res=0;
    if(insulationResistance!=null && insulationResistance <= 40000 ){
      res=2;
    }else if(insulationResistance!=null && insulationResistance <= 100000 ){
      res=1;
    }
    res
  }

}
