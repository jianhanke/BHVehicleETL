package com.neuexample.entry

class Alarm(vinx :String,ctimex :Long,alarm_typex :String ,alarm_valx :Int,areax :String,cityx :String,provincex :String,regionx :String,levelx :Int,vehicle_factoryx:Int,
  chargeStatusx:Int,mileagex:Long,voltagex:Int,currentx:Int,socx:Int,dcStatusx:Int,insulationResistancex:Int,maxVoltageSystemNumx:Int,
            maxVoltagebatteryNumx:Int,batteryMaxVoltagex:Int ,minVoltageSystemNumx:Int,minVoltagebatteryNumx:Int,
            batteryMinVoltagex:Int,maxTemperatureSystemNumx:Int,maxTemperatureNumx:Int,maxTemperaturex:Int,minTemperatureSystemNumx:Int,
            minTemperatureNumx:Int,minTemperaturex:Int,temperatureProbeCountx:Int,probeTemperaturesx:String,cellCountx:Int,cellVoltagesx:String,
            total_voltage_drop_ratex:Double,max_temperature_heating_ratex:Double,soc_high_valuex:Double,soc_diff_valuex:Double,
            soc_jump_valuex:Double,soc_jump_timex:Int,battery_standing_timex:Int,temperature_diffx:Int,insulation_om_vx:Double,
            voltage_uppder_boundaryx:Double,voltage_down_boundaryx:Double,temperature_uppder_boundaryx:Double,temperature_down_boundaryx:Double,
            soc_notbalance_timex:Int,soc_high_timex:Int
           ) extends  Serializable {
  val vin :String = vinx
  var start_time: Long = ctimex
  var ctime: Long = ctimex
  val alarm_type :String = alarm_typex
  val alarm_val :Int = alarm_valx
  val area :String = areax
  val city :String = cityx
  val province :String = provincex
  val region :String = regionx
  var level :Int = levelx
  var last_start_time: Long = ctimex
  var end_time: Long = ctime
  val vehicle_factory:Int = vehicle_factoryx

  val chargeStatus:Int=chargeStatusx
  val mileage:Long=mileagex
  val voltage:Int=voltagex
  val current:Int=currentx
  val soc:Int=socx
  val dcStatus:Int=dcStatusx
  val insulationResistance:Int=insulationResistancex
  val maxVoltageSystemNum:Int=maxVoltageSystemNumx
  val maxVoltagebatteryNum:Int=maxVoltagebatteryNumx
  val batteryMaxVoltage:Int=batteryMaxVoltagex
  val minVoltageSystemNum:Int=minVoltageSystemNumx
  val minVoltagebatteryNum:Int=minVoltagebatteryNumx
  val batteryMinVoltage:Int=batteryMinVoltagex
  val maxTemperatureSystemNum:Int=maxTemperatureSystemNumx
  val maxTemperatureNum:Int=maxTemperatureNumx
  val maxTemperature:Int=maxTemperaturex
  val minTemperatureSystemNum:Int=minTemperatureSystemNumx
  val minTemperatureNum:Int=minTemperatureNumx
  val minTemperature:Int=minTemperaturex
  val temperatureProbeCount:Int=temperatureProbeCountx
  val probeTemperatures:String=probeTemperaturesx
  val cellCount:Int=cellCountx
  val cellVoltages:String=cellVoltagesx

  val total_voltage_drop_rate:Double=total_voltage_drop_ratex
  val max_temperature_heating_rate:Double=max_temperature_heating_ratex
  val soc_high_value:Double=soc_high_valuex
  val soc_diff_value:Double=soc_diff_valuex
  val soc_jump_value:Double=soc_jump_valuex
  val soc_jump_time:Int=soc_jump_timex
  val battery_standing_time:Int=battery_standing_timex
  val temperature_diff:Int=temperature_diffx
  val insulation_om_v:Double=insulation_om_vx
  val voltage_uppder_boundary:Double=voltage_uppder_boundaryx
  val voltage_down_boundary:Double=voltage_down_boundaryx
  val temperature_uppder_boundary:Double=temperature_uppder_boundaryx
  val temperature_down_boundary:Double=temperature_down_boundaryx
  val soc_notbalance_time:Int=soc_notbalance_timex;
  val soc_high_time:Int=soc_high_timex;

}
