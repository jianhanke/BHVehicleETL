package com.neuexample.Test

import com.neuexample.streaming.WarningSteaming.properties
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc =  new StreamingContext(sc, batchDuration = Seconds(2))
    ssc.checkpoint(properties.getProperty("C://abc2"));

    val textRDD: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\00000000000046246109.log")

    val timeDiffRDD: RDD[Unit] = textRDD.map {
      line => {

        val startIndex: Int = line.indexOf("{")
        val str: String = line.substring(startIndex)

        println(str)
      }
    }
    timeDiffRDD.count()



  }

}
