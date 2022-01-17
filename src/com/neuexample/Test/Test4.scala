package com.neuexample.Test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object Test4 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val ds: Dataset[String] = spark.read.textFile("C:\\Users\\lenovo\\Desktop\\text.txt");

    ds.show()

    val df: DataFrame = ds.map(x => {
      val splits: Array[String] = x.split(",")
      (splits(0), splits(1))
    }).toDF("name", "age")
      .cache()




    df.withColumn("sno",lit("22"))
      .withColumnRenamed("name","rename")
      .show(false)


    df.withColumn("sno1",'age+1).show()

    df.withColumn("sno2",$"age").show()

    df.withColumn("sno3",col("age")+2).show()

    df.withColumn("sno4",column("age")+3)
      .withColumn("sno4",$"sno4".cast(IntegerType)).show()


    df.limit(1)

    // df.withColumn("sno5",lag(col("age"),0,65535)).show()

  }


}
