package com.neuexample.Test

object MyTest {

  def main(args: Array[String]): Unit = {



    val array = Array(("1","2","3","4",5.0),("1","2","3","4",6.0))

    val tuple1: Array[(String, String, String, String, Double)] = array.take(1)

    val tuple2: (String, String, String, String, Double) = array(0)

    println(tuple1.mkString(","))
    println(tuple2.toString())





  }

}
