package com.neuexample.Test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



case class Salary(depName: String, empNo: Long, name: String, salary: Long, hobby: Seq[String])

object Test {


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

    empsalary.show()


    // val overCategory = Window.partitionBy('depName)
    val overCategory: WindowSpec = Window.partitionBy("depName").rowsBetween(Window.currentRow,2)

    // 基于窗口求平均值，和，最大值，最小值
    val df: DataFrame = empsalary.withColumn("totalSalaryList", collect_list("salary") over overCategory)
      .withColumn("avgSalary", (avg("salary") over overCategory).cast("int"))
      .withColumn("minSalary", min("salary") over overCategory)
      .withColumn("maxSalary", max("salary") over overCategory)
      .withColumn("sumSalary", sum("salary") over overCategory)
      .withColumn("dsaf",  col("salary"))


    val value: Dataset[Salary] = df.as[Salary]
    value.show(false)

    df.show()



     val column: Column = df.apply("salary")
    val salary: Column = df("salary")

    df.orderBy($"salary".asc).show()


    val overCategory2: WindowSpec = Window.partitionBy("depName").orderBy("salary").rangeBetween(Window.unboundedPreceding,300);

    empsalary.withColumn("salaries",collect_list("salary") over overCategory2 )
      .withColumn("sumSalary" ,sum("salary") over overCategory2 )
      .show(false)


    val overCategory3: WindowSpec = Window.partitionBy("depName").orderBy("salary")

    val frame: DataFrame = empsalary.withColumn("fsdssalary", lead("salary", 2) over overCategory3)
      .withColumn("test", $"salary")


    frame
        .withColumn("addFiled", lit("321") )
        .where("fsdssalary is null ")
      .show(false)







  }


}
