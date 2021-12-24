package com.neuexample.Test

import com.neuexample.streaming.WarningSteaming.properties
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TextApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR");
    val ssc =  new StreamingContext(sc, batchDuration = Seconds(2))
    ssc.checkpoint(properties.getProperty("C://abc"));
    val accumulator: Accumulator[Int] = sc.accumulator(0)

    val line_record: Accumulator[Int] = sc.accumulator(0)

    val fu_sum: Accumulator[Int] = sc.accumulator(0)

    val zhengchang: Accumulator[Int] = sc.accumulator(0)


    val zheng_sum: Accumulator[Int] = sc.accumulator(0)

    val textRDD: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\2.log")

    val timeDiffRDD: RDD[Unit] = textRDD.map {
      line => {

        if(line.length>7) {
          val str: String = line.substring(0, 7)
          if (str.equals("curTime")) {

            line_record+=1;

            val arr: Array[String] = line.split(",")
            if (arr.length == 3) {
              val arr2: Array[String] = arr(2).split(":")
              if (arr2.length == 2) {
                val int: Int = arr2(1).toInt
                if (int < -300) {
                  accumulator+=1;
                }
                if(int>300){
                  zheng_sum+=1;
                }
                if( int > 0 && int <=10 ){
                  zhengchang+=1;
                }

                if(int <0){
                  fu_sum+=1;
                }
              }
            }
          }
        }

      }
    }
    timeDiffRDD.count()
    println("zheng:"+zheng_sum);
    println("line_record:"+line_record);
    println("accumularot:"+accumulator);
    println("负数:"+fu_sum);

    println("正常:"+zhengchang);


  }

}
