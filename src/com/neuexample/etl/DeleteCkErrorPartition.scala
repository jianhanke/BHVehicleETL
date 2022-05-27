package com.neuexample.etl

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.neuexample.utils.GetConfig

object DeleteCkErrorPartition  {

  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val churl =  "jdbc:clickhouse://8.142.174.40:8123/source_gx?socket_timeout=3000000"
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")

    var chproperties: Properties = new Properties()
    chproperties.put("url", churl)
    chproperties.put("user","default")
    chproperties.put("password","")
    chproperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")

    val connection = DriverManager.getConnection(churl,chproperties)


    var dataBaseWithtable="source_gx.ods_data";

    println("SELECT distinct(day_of_year) FROM %s group by day_of_year".format(dataBaseWithtable))

    val resultSet: ResultSet = connection.createStatement().executeQuery("SELECT distinct(day_of_year) FROM %s group by day_of_year".format(dataBaseWithtable))



     val format = new SimpleDateFormat("yyyy-MM-dd")

    val date: Date = format.parse("2022-0222-02")


    val set = new util.HashSet[String]()

    while (resultSet.next()){
      val day_of_year: String = resultSet.getString(1)
      val array: Array[String] = day_of_year.split("-")

//      if(array(0) == 21 || array(0) == 22){
//
//      }
       if( array(0).toInt != 2021 && array(0).toInt != 2022 ){
        println("alter table   %s drop PARTITION   '%s';".format(dataBaseWithtable, day_of_year))
      }else if(array(1).toInt == 0 ||  array(1).toInt > 12){
         println("alter table   %s drop PARTITION   '%s';".format(dataBaseWithtable, day_of_year))
      }else if(array(2).toInt == 0 || array(2).toInt > 31){
         println("alter table   %s drop PARTITION   '%s';".format(dataBaseWithtable, day_of_year))
      }


    }


  }




}
