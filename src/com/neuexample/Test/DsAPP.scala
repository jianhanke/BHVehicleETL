package com.neuexample.Test

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DsAPP {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("StreamingVehicleTrip")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    val empsalary = Seq(
      Salary("sales",     1,  "Alice",  5000, List("game",  "ski")),
      Salary("personnel", 2,  "Olivia", 3900, List("game",  "ski")),
      Salary("sales",     3,  "Ella",   4800, List("skate", "ski")),
      Salary("sales",     4,  "Ebba",   4800, List("game",  "ski")),
      Salary("personnel", 5,  "Lilly",  3500, List("climb", "ski")),
      Salary("develop",   7,  "Astrid", 4200, List("game",  "ski")),
      Salary("develop",   8,  "Saga",   6000, List("kajak", "ski")),
      Salary("develop",   9,  "Freja",  4500, List("game",  "kajak")),
      Salary("develop",   10, "Wilma",  5200, List("game",  "ski")),
      Salary("develop",   11, "Maja",   5200, List("game",  "farming"))).toDS

    val w1: WindowSpec = Window.partitionBy("depName").orderBy($"salary".desc )

    empsalary.withColumn("sum_salary",lag("salary",1,0) over w1  )
      //.withColumn("toatl_salary", col("salary")+col("last_salary") )
      .show()

  }
}
