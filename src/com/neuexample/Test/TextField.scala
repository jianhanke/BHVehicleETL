package com.neuexample.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TextField {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\new 7312321.txt")

    rdd.foreach(

      line=>{

        val splits: Array[String] = line.split(" ")
        val field: String = splits(0).trim

        print(  "\"%s\",".format(field) )

      }
    )



  }

}
