package com.neuexample.etl

import com.neuexample.utils.GetConfig

import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

object FindDifferentCount  {

  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val churl = properties.getProperty("clickhouse.conn")
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")

    var chproperties: Properties = new Properties()
    chproperties.put("url", churl)
    chproperties.put("user","gotion_buaa_ckdatabase")
    chproperties.put("password","Neu_BH!")
    chproperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")

    val connection = DriverManager.getConnection(churl,chproperties)
    val hashMap = new util.HashMap[String, Int]()
    val set = new util.HashSet[String]()

    val result1: ResultSet = connection.createStatement().executeQuery("select day_of_year ,count(*) from warehouse_gx.vehicle_dwd where  day_of_year <= '2022-04-30'  group by day_of_year")
    while (result1.next()){
      val day_of_year: String = result1.getString(1)
      val count = result1.getInt(2)
      hashMap.put(day_of_year,count);
      set.add(day_of_year)
    }



    var chproperties2: Properties = new Properties()
    val churl2 = "jdbc:clickhouse://8.142.171.232:8123/warehouse_gx?socket_timeout=3000000"
    chproperties2.put("url", churl2)
    chproperties2.put("user","default")
    chproperties2.put("password","")
    chproperties2.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")

    val connection2 = DriverManager.getConnection(churl2,chproperties2)

    val result2: ResultSet = connection2.createStatement().executeQuery("select day_of_year ,count(*) from warehouse_gx.vehicle_dwd group by day_of_year")

    while (result2.next()){
      val day_of_year: String = result2.getString(1)
      val count = result2.getInt(2)
      val mapCount = hashMap.get(day_of_year)
      if(mapCount != count){
        println("alter table   %s drop PARTITION   '%s';".format("warehouse_gx.vehicle_dwd", day_of_year))
      }
      set.remove(day_of_year)
    }

    val isterator = set.iterator()
    while(isterator.hasNext){
      val next = isterator.next()
      var sql="INSERT INTO warehouse_gx.vehicle_dwd SELECT * FROM remote('192.168.231.75:9000', 'warehouse_gx', 'vehicle_dwd', 'gotion_buaa_ckdatabase', 'Neu_BH!')　where  day_of_year = '%s';".format(next)
      println(sql)
    }

    println(result1.getRow)
    println(result2.getRow)

  }
}
