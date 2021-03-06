package com.neuexample.entry



case class Vehicle(

                    alarminfo: Long,
                    batteryconsistencypoor: Boolean,
                    batterycount: Long,
                    batteryhightemperature: Boolean,
                    batterymaxvoltage: Long,
                    batteryminvoltage: Long,
                    batterynumber: Long,
                    brakingsystem: Boolean,
                    cellcount: Long,
                    cellvoltages: List[Long],
                    chargestatus: Long,
                    controllerdcbusbarcurrent: Long,
                    controllerinputvoltage: Long,
                    current: Long,
                    day: Long,
                    dcstatus: Boolean,
                    dctemperature: Boolean,
                    devicefailurescodes: List[Long],
                    devicefailurescount: Long,
                    devicetypedontmatch: Boolean,
                    devicetypeoverfilling: Boolean,
                    devicetypeovervoltage: Boolean,
                    devicetypeundervoltage: Boolean,
                    drivemotorcontrollertemperature: Boolean,
                    drivemotorcount: Long,
                    drivemotorfailurescodes: List[Long],
                    drivemotorfailurescount: Long,
                    drivemotortemperature: Boolean,
                    enginefailurescodes: List[Long],
                    enginefailurescount: Long,
                    gears: Long,
                    highpressureinterlock: Boolean,
                    hours: Long,
                    insulation: Boolean,
                    insulationresistance: Long,
                    latitude: Long,
                    level: Long,
                    longitude: Long,
                    maxtemperature: Long,
                    maxtemperaturenum: Long,
                    maxtemperaturesystemnum: Long,
                    maxvoltagesystemnum: Long,
                    maxvoltagebatterynum: Long,
                    mileage: Long,
                    mintemperature: Long,
                    mintemperaturenum: Long,
                    mintemperaturesystemnum: Long,
                    minvoltagesystemnum: Long,
                    minvoltagebatterynum: Long,
                    minutes: Long,
                    monomerbatteryovervoltage: Boolean,
                    monomerbatteryundervoltage: Boolean,
                    month: Long,
                    operationmode: Long,
                    otherfailurescodes: List[Long],
                    otherfailurescount: Long,
                    probetemperatures: List[Long],
                    rivemotordatanum: Long,
                    seconds: Long,
                    soc: Long,
                    sochigh: Boolean,
                    socjump: Boolean,
                    soclow: Boolean,
                    speed: Long,
                    status: Long,
                    subsystemtemperaturecount: Long,
                    subsystemtemperaturedatanum: Long,
                    subsystemvoltagecount: Long,
                    subsystemvoltagedatanum: Long,
                    temperature: Long,
                    temperaturedifferential: Boolean,
                    temperatureprobecount: Long,
                    time: Long,
                    torque: Long,
                    totalcurrent: Long,
                    totalvoltage: Long,
                    vehiclefactory: String,
                    vehiclestatus: Long,
                    vin: String,
                    voltage: Long,
                    year: Long,

                    ctime: Long
                    // alarm_type: String

                  ) extends  Serializable{



}
