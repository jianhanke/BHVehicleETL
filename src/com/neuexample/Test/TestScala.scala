package com.neuexample.Test

import java.text.SimpleDateFormat

object TestScala {


  def main(args: Array[String]): Unit = {

    println(mkctime(22, 2, 17, 2, 2, 2))

  }

  def mkctime (year:Int,month:Int,day:Int,hours:Int,minutes:Int,seconds:Int) :Long ={

      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("20%02d-%02d-%02d %02d:%02d:%02d".format(year, month, day, hours, minutes, seconds)).getTime / 1000

  }


}
