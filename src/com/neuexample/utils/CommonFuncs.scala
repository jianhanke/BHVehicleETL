package com.neuexample.utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, pow, udf}

import java.text.SimpleDateFormat

object CommonFuncs {

  /**
    *
    * @param str  ["1","2","3]
    * @return      Array[1,2,3]
    */
  def stringToIntArray(str:String):Array[Int]={

    if(str!=null && str.length>2) {
      val strArr: Array[String] = str.substring(1, str.length - 1).split(",")
      val intArr = new Array[Int](strArr.length);
      for (i <- 0 until intArr.length) {
        intArr(i) = strArr(i).toInt
      }
      return  intArr
    }
    null
  }

  /*
  *日期转时间戳
   */
  def getTimestamp(x : String) : Long  = {
    val format = new SimpleDateFormat("yyyyMMdd")
    format.parse(x).getTime/1000
  }
  /*
    *func:组合被拆解的ctime
     */
  val udf_mkctime = udf((year:Int,month:Int,day:Int,hours:Int,minutes:Int,seconds:Int)=>{
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("20%s-%02d-%02d %02d:%02d:%02d".format(year,month,day,hours,minutes,seconds)).getTime/1000
  })
  /*
      *func:组合被拆解的ctime,function版
       */
  def mkctime (year:Int,month:Int,day:Int,hours:Int,minutes:Int,seconds:Int) :Long ={
    //println("year:"+year+",month:"+month+",day:"+day+",hours:"+hours+",minutes:"+minutes+",seconds"+seconds);
    try {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("20%s-%02d-%02d %02d:%02d:%02d".format(year, month, day, hours, minutes, seconds)).getTime / 1000
    }catch {
      case e=> return 0;
    }
  }

  /*
  *数组内最大值
   */
  val getArrayMax = udf((data: Seq[Integer]) =>{
      if (data.length == 0) {
        new Integer(0)
      } else {
        data.max
      }
  })

  /*
  *数组内最小值
   */
  val getArrayMin = udf((data: Seq[Integer]) =>{
    if (data.length == 0){
      new Integer(0)
    }else {
      data.min
    }
  })

  /*
  *获取经纬度DataFrame
   */
  def locateCity(lon :Double,lat:Double,df_gps :DataFrame): String ={
    val tmp_df = df_gps
    val out_df = tmp_df.withColumn("distance",pow(col("lat") - lat,2) + pow(col("lon") - lon,2))
      .orderBy(col("distance"))
      .limit(1)
      .select("area","city","province","region")
    out_df.collect()(0).mkString(",")
  }
  /*
    *获取经纬度RDD
     */
  def locateCityRDD(lon :Double ,lat :Double,gps_rdd :Array[String]) :String={
    val current_place = gps_rdd.map(rdd =>{
      val splits = rdd.split(",")
      val llat = splits(3).toDouble
      val llon = splits(4).toDouble
      (splits(0),splits(1),splits(2),splits(5),(llat - lat) * (llat - lat) + (llon - lon) * (llon - lon))
    }).sortBy(x => x._5)
      .take(1)
    current_place.mkString(",")
  }
}
