package com.neuexample.entry

case class OfflineAlarm(

           vin: String,
           start_time: Long,
           ctime: Long,
           alarm_type: String,
           alarm_val: Int,
           area: String,
           city: String,
           province: String,
           region: String,
           level: Int,
           last_start_time: Long,
           end_time: Long,
           vehicleFactory: Int,

           chargeStatus: Int,
           mileage: Long,
           voltage: Int,
           current:  Int,
           soc: Int,
           dcStatus: Int,
           insulationResistance: Int,
           maxVoltageSystemNum: Int,
           maxVoltagebatteryNum: Int,
           batteryMaxVoltage: Int,
           minVoltageSystemNum: Int,
           minVoltagebatteryNum: Int,
           batteryMinVoltage: Int,
           maxTemperatureSystemNum: Int,
           maxTemperatureNum: Int,
           maxTemperature: Int,
           minTemperatureSystemNum: Int,
           minTemperatureNum: Int,
           minTemperature: Int,
           temperatureProbeCount: Int,
           probeTemperatures: String,
           cellCount: Int,
           cellVoltages: String,

           longitude: Long,
           latitude: Long,
           speed: Int

                       ) extends  Serializable {






}
