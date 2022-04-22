package com.neuexample.Test

import com.neuexample.utils.GetConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogApp2 {

  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val spark = SparkSession
      .builder
      .master(properties.getProperty("spark.master"))
      .appName("SparkStreamingKafkaDirexct")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc =  new StreamingContext(sc, batchDuration = Seconds(2))
    ssc.checkpoint(properties.getProperty("C://abc2"));

    val textRDD: RDD[String] = sc.textFile("C:\\Users\\lenovo\\Desktop\\新建文本文档 (2).txt")

    val value: RDD[(String, Int)] = textRDD.map {
      line => {

        val array: Array[String] = line.split("\t")
        val paths: Array[String] = array(1).split("/")

        (array(0), paths(4).toInt)
      }
    }


    value.repartition(1).sortBy(_._2).foreach(println(_))

  }

}
