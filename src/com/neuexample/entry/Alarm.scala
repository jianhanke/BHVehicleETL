package com.neuexample.entry


class Alarm() extends  Serializable {

  var vin:String =  ""
  var start_time:Long = 0
  var ctime:Long = 0
  var alarm_type:String =  ""
  var alarm_val:Int = 0
  var area:String =  ""
  var city:String =  ""
  var province:String =  ""
  var region:String =  ""
  var level:Int = 0
  var last_start_time:Long = 0
  var end_time:Long = 0
  var vehicleFactory:Int = 0

  var chargeStatus:Int = 0
  var mileage:Long = 0
  var voltage:Int = 0
  var current:Int = 0
  var soc:Int = 0
  var dcStatus:Int = 0
  var insulationResistance:Int = 0
  var maxVoltageSystemNum:Int = 0
  var maxVoltagebatteryNum:Int = 0
  var batteryMaxVoltage:Int = 0
  var minVoltageSystemNum:Int = 0
  var minVoltagebatteryNum:Int = 0
  var batteryMinVoltage:Int = 0
  var maxTemperatureSystemNum:Int = 0
  var maxTemperatureNum:Int = 0
  var maxTemperature:Int = 0
  var minTemperatureSystemNum:Int = 0
  var minTemperatureNum:Int = 0
  var minTemperature:Int = 0
  var temperatureProbeCount:Int = 0
  var probeTemperatures:String =  ""
  var cellCount:Int = 0
  var cellVoltages:String =  ""

  var total_voltage_drop_rate:Double = 0
  var max_temperature_heating_rate:Double = 0
  var soc_high_value:Double = 0
  var soc_diff_value:Double = 0
  var soc_jump_value:Double = 0
  var soc_jump_time:Int = 0
  var battery_standing_time:Int = 0
  var temperature_diff:Int = 0
  var insulation_om_v:Double = 0
  var voltage_uppder_boundary:Double = 0
  var voltage_down_boundary:Double = 0
  var temperature_uppder_boundary:Double = 0
  var temperature_down_boundary:Double = 0
  var soc_notbalance_time:Int = 0
  var soc_high_time:Int = 0

  var last_alarm_time:Long = 0
  var longitude:Long = 0
  var latitude:Long = 0
  var speed:Int = 0


  def setVin(vin: String): Unit = {
    this.vin = vin
  }

  def setSpeed(speed: Int): Unit ={
    this.speed = speed
  }

  def setLongitude(longitude: Long): Unit ={
    this.longitude = longitude
  }

  def setLatitude(latitude: Long): Unit ={
    this.latitude = latitude
  }


  def setCtime(ctime: Long): Unit = {
    this.ctime = ctime
    this.start_time = ctime
    this.end_time = ctime
  }

  def setLast_start_time(last_start_time: Long): Unit ={
    this.last_start_time = last_start_time
  }

  def setLast_alarm_time(last_alarm_time: Long): Unit ={
    this.last_alarm_time = last_alarm_time
  }



  def setAlarm_type(alarm_type: String): Unit = {
    this.alarm_type = alarm_type
  }

  def setAlarm_val(alarm_val: Int): Unit = {
    this.alarm_val = alarm_val
  }

  def setArea(area: String): Unit = {
    this.area = area
  }

  def setCity(city: String): Unit = {
    this.city = city
  }

  def setProvince(province: String): Unit = {
    this.province = province
  }

  def setRegion(region: String): Unit = {
    this.region = region
  }

  def setLevel(level: Int): Unit = {
    this.level = level
  }



  def setVehicleFactory(vehicleFactory: Int): Unit = {
    this.vehicleFactory = vehicleFactory
  }

  def setChargeStatus(chargeStatus: Int): Unit = {
    this.chargeStatus = chargeStatus
  }

  def setMileage(mileage: Long): Unit = {
    this.mileage = mileage
  }

  def setVoltage(voltage: Int): Unit = {
    this.voltage = voltage
  }

  def setCurrent(current: Int): Unit = {
    this.current = current
  }

  def setSoc(soc: Int): Unit = {
    this.soc = soc
  }

  def setDcStatus(dcStatus: Int): Unit = {
    this.dcStatus = dcStatus
  }

  def setInsulationResistance(insulationResistance: Int): Unit = {
    this.insulationResistance = insulationResistance
  }

  def setMaxVoltageSystemNum(maxVoltageSystemNum: Int): Unit = {
    this.maxVoltageSystemNum = maxVoltageSystemNum
  }

  def setMaxVoltagebatteryNum(maxVoltagebatteryNum: Int): Unit = {
    this.maxVoltagebatteryNum = maxVoltagebatteryNum
  }

  def setBatteryMaxVoltage(batteryMaxVoltage: Int): Unit = {
    this.batteryMaxVoltage = batteryMaxVoltage
  }

  def setMinVoltageSystemNum(minVoltageSystemNum: Int): Unit = {
    this.minVoltageSystemNum = minVoltageSystemNum
  }

  def setMinVoltagebatteryNum(minVoltagebatteryNum: Int): Unit = {
    this.minVoltagebatteryNum = minVoltagebatteryNum
  }

  def setBatteryMinVoltage(batteryMinVoltage: Int): Unit = {
    this.batteryMinVoltage = batteryMinVoltage
  }

  def setMaxTemperatureSystemNum(maxTemperatureSystemNum: Int): Unit = {
    this.maxTemperatureSystemNum = maxTemperatureSystemNum
  }

  def setMaxTemperatureNum(maxTemperatureNum: Int): Unit = {
    this.maxTemperatureNum = maxTemperatureNum
  }

  def setMaxTemperature(maxTemperature: Int): Unit = {
    this.maxTemperature = maxTemperature
  }

  def setMinTemperatureSystemNum(minTemperatureSystemNum: Int): Unit = {
    this.minTemperatureSystemNum = minTemperatureSystemNum
  }

  def setMinTemperatureNum(minTemperatureNum: Int): Unit = {
    this.minTemperatureNum = minTemperatureNum
  }

  def setMinTemperature(minTemperature: Int): Unit = {
    this.minTemperature = minTemperature
  }

  def setTemperatureProbeCount(temperatureProbeCount: Int): Unit = {
    this.temperatureProbeCount = temperatureProbeCount
  }

  def setProbeTemperatures(probeTemperatures: String): Unit = {
    this.probeTemperatures = probeTemperatures
  }

  def setCellCount(cellCount: Int): Unit = {
    this.cellCount = cellCount
  }

  def setCellVoltages(cellVoltages: String): Unit = {
    this.cellVoltages = cellVoltages
  }

  def setTotal_voltage_drop_rate(total_voltage_drop_rate: Double): Unit = {
    this.total_voltage_drop_rate = total_voltage_drop_rate
  }

  def setMax_temperature_heating_rate(max_temperature_heating_rate: Double): Unit = {
    this.max_temperature_heating_rate = max_temperature_heating_rate
  }

  def setSoc_high_value(soc_high_value: Double): Unit = {
    this.soc_high_value = soc_high_value
  }

  def setSoc_diff_value(soc_diff_value: Double): Unit = {
    this.soc_diff_value = soc_diff_value
  }

  def setSoc_jump_value(soc_jump_value: Double): Unit = {
    this.soc_jump_value = soc_jump_value
  }

  def setSoc_jump_time(soc_jump_time: Int): Unit = {
    this.soc_jump_time = soc_jump_time
  }

  def setBattery_standing_time(battery_standing_time: Int): Unit = {
    this.battery_standing_time = battery_standing_time
  }

  def setTemperature_diff(temperature_diff: Int): Unit = {
    this.temperature_diff = temperature_diff
  }

  def setInsulation_om_v(insulation_om_v: Double): Unit = {
    this.insulation_om_v = insulation_om_v
  }

  def setVoltage_uppder_boundary(voltage_uppder_boundary: Double): Unit = {
    this.voltage_uppder_boundary = voltage_uppder_boundary
  }

  def setVoltage_down_boundary(voltage_down_boundary: Double): Unit = {
    this.voltage_down_boundary = voltage_down_boundary
  }

  def setTemperature_uppder_boundary(temperature_uppder_boundary: Double): Unit = {
    this.temperature_uppder_boundary = temperature_uppder_boundary
  }

  def setTemperature_down_boundary(temperature_down_boundary: Double): Unit = {
    this.temperature_down_boundary = temperature_down_boundary
  }

  def setSoc_notbalance_time(soc_notbalance_time: Int): Unit = {
    this.soc_notbalance_time = soc_notbalance_time
  }

  def setSoc_high_time(soc_high_time: Int): Unit = {
    this.soc_high_time = soc_high_time
  }

}
