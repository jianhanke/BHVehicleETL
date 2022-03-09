package com.neuexample.etl

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

import com.neuexample.utils.GetConfig

object IssueCertificate  {

  def main(args: Array[String]): Unit = {

    val properties = GetConfig.getProperties("test.properties")

    val churl = properties.getProperty("clickhouse.conn")
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")

    var chproperties: Properties = new Properties()
    chproperties.put("url", churl)
    chproperties.put("user","default")
    chproperties.put("password","Neu_BH!")
    chproperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")

    val connection = DriverManager.getConnection(churl,chproperties)

    val resultSet: ResultSet = connection.createStatement().executeQuery("select distinct(vin) from dwd_vehicledata_detail")

    while (resultSet.next()){
      val vin: String = resultSet.getString(1)
      println(vin)
    }
    println(resultSet.getRow)

  }




}
