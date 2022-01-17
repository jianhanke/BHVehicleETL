package com.neuexample.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadJson {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\new 7.txt")

      rdd.foreach(  line=>{
          val splits: Array[String] = line.split(" ")
        var str="";
        if( splits(1).equals("bigint") ){
              str="Long";
        }else if( splits(1).equals("boolean") ){
          str="Boolean";
        }else if( splits(1).equals("array<bigint>") ){
          str="List[Long]";
        }else if( splits(1).equals("string") ){
          str="String";
        }
        print(splits(0)+": "+str+", \n")

    } )


  }

}
