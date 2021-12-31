package com.neuexample.structured

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount1 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WorldCount")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "154.8.173.46")
      .option("port", 9999)
      .load()

    val result: StreamingQuery = lines.writeStream
      .format("console")
      .outputMode("update")
      .start

    result.awaitTermination()
    spark.stop()

  }

}
