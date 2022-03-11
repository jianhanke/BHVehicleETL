package com.neuexample.Test

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.neuexample.entry.Alarm

import scala.collection.mutable

object TestBean {


  def main(args: Array[String]): Unit = {

      var string2="{\"age\":3}";
      val json2: JSONObject = JSON.parseObject(string2)

    import scala.collection.mutable

    var sets = mutable.Set[Integer]();
    sets +=1;
    sets +=2;
    for(i <- sets){
      println(i)
    }




      var alarm = new Alarm()
      alarm.level=100
      alarm.province = String.valueOf(100)
      val list = new util.ArrayList[Alarm]

      for(i <- 0 until  10){
        var clone: Alarm = alarm.clone()
        clone.province = String.valueOf(i);
        clone.level = i;
        list.add(clone)
      }
      for(i <- 0 until 10){
        println(list.get(i).level + "," + list.get(i).province  )
      }

    var string = "{}";
    val json: JSONObject = JSON.parseObject(string)

    val values = new JSONObject()
    values.put("age",1)

    json.put("my_json",values.toString);


    val my_json: JSONObject = json.getJSONObject("my_json")
    my_json.put("age",3)



    println(json)

  }

}
