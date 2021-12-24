package com.neuexample.etl
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import com.neuexample.utils.CommonFuncs._
import com.neuexample.utils.GetConfig

import java.sql.DriverManager
import java.util.Properties



object TripDivide {
  /**
   * func:清理原始数据中跳变的数据，如charge_state 3，3，3，3，3，1，3，3，3
   */
  def clean_charge_state(df: DataFrame) = {

    val w = Window.partitionBy("vin").orderBy(col("ctime"))
    df.withColumn("first_charge_state",lag(col("charge_state"),1,65535).over(w))
      .withColumn("last_charge_state",lead(col("charge_state"),1,65535).over(w))
      .withColumn("charge_state",when((col("charge_state") =!= col("last_charge_state"))
              && (col("charge_state") =!= col("first_charge_state"))
        && (col("first_charge_state") === col("last_charge_state")),col("last_charge_state"))
      .otherwise(col("charge_state")))
      .drop(col("first_charge_state"))
      .drop(col("last_charge_state"))
 }

  /*
  *func:便于处理，将charge_state规划为1--充电，2--放电
   */
  val udf_charge_state = udf((charge_state_org: Int) => {
    if (charge_state_org == 1 || charge_state_org == 4) {
      1
    } else {
      2
    }
  })

  /*
  *将未结束的行程明细，写出到tmp_part_ods_vehicledata
   */
  def writeTmpTbl(trip_detail_df :DataFrame): Unit ={
    val spark = trip_detail_df.sparkSession
    import spark.implicits._
    //当前范围内数据中，未结束行程写出到临时表
    trip_detail_df
      .where("trip_complete_flag = 0")
      .drop("end_time")
      .drop("trip_complete_flag")
      .drop("ctime")
      .drop("charge_state")
      .repartition($"yyyymmdd")
      .write
      .mode("OverWrite")
      .saveAsTable("source_gx.tmp_part_ods_vehicledata_tmp")
    //临时表写回tmp_part_ods_vehicledata表
    spark.sql("select * from source_gx.tmp_part_ods_vehicledata_tmp")
      .repartition($"yyyymmdd")
      .write
      .mode("OverWrite")
      .saveAsTable("source_gx.tmp_part_ods_vehicledata")
    spark.sql("drop table if exists source_gx.tmp_part_ods_vehicledata_tmp")
  }

  /*
  *新行程写入行程表
   */
  def writeTripTbl(df :DataFrame)={
    df
      .repartition(col("day_of_year"))
      .write
      .mode("Append")
      .saveAsTable("source_gx.dw_tripdivide")
  }

  /*
  *新行程写入clickhouse表
   */
  def writeTripClickhouseTbl(df :DataFrame,properties: Properties)={
    val churl = properties.getProperty("clickhouse.conn")
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    var chproperties: Properties = new Properties()
    chproperties.put("url", churl)
    chproperties.put("user","default")
    chproperties.put("password","Neu_BH!")
    chproperties.put("driver", "ru.yandex.clickhouse.ClickHouseDriver")
    val connection = DriverManager.getConnection(churl,chproperties)
    connection.createStatement().executeUpdate("truncate table %s.%s".format("source_gx", "dw_tripdivide"))
    df
      .na.fill(0)
      .write
      .mode("Append")
      .option("batchsize", "50000")
      .option("isolationLevel", "NONE")
      .option("numPartitions", "1")
      .jdbc(churl, String.format("%s.%s", "source_gx", "dw_tripdivide"), chproperties)
  }

  def main(args :Array[String])={
    assert(args.length == 3,message="运行方式如下：spark-submit.sh --class com.neuexample.etl.TripDivide BHVehicleETL-1.0-SNAPSHOT.jar [start_date(yyyymmdd)] [end_date(yyyymmdd,not include)] [master(local|yarn|yarn-cluster)]")
    val properties = GetConfig.getProperties("test.properties")
    val s_yyyymmdd = args(0)
    val e_yyyymmdd = args(1)
    //两条数据间隔10分钟，认为旧行程结束，新行程开始
    val BREAK_DURATION = 600
    //行程end_time距e_yyyymmdd时间超过10分钟，认为行程已经终止
    val TRIP_END_FLAG = 600
    val time_max = getTimestamp(e_yyyymmdd)

    val spark = SparkSession
      .builder()
      .appName("StreamingVehicleTrip")
      .master(args(2))
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    spark.sql("create table if not exists source_gx.tmp_part_ods_vehicledata like source_gx.ods_vehicledata")
    val detail_data = spark.sql("select * from source_gx.ods_vehicledata where yyyymmdd >= '%s' and yyyymmdd<'%s' ".format(s_yyyymmdd,e_yyyymmdd))
      .union(spark.sql("select * from source_gx.tmp_part_ods_vehicledata"))
      .withColumn("ctime",udf_mkctime(col("year"),col("month"),col("day"),col("hours"),col("minutes"),col("seconds")))
      .withColumn("charge_state",udf_charge_state($"chargeStatus".cast(IntegerType)))
    val df_clean = clean_charge_state(detail_data)

    val w1 = Window.partitionBy("vin", "charge_state").orderBy("ctime")
    val w2 = Window.partitionBy("vin").orderBy("ctime")
    val w3 = Window.partitionBy("vin").orderBy("end_time")

    val df_end = df_clean
      .withColumn("ctime_diff", col("ctime") - lag("ctime", 1, Long.MaxValue).over(w1))
      .withColumn("charge_state_change", abs(lead("charge_state", 1, Long.MaxValue).over(w2) - col("charge_state"))) //vin分组
      //取出所有的end_time作为一个DF，用于行程划分
      //end_time_pre是上次行程的end_time
      .filter(col("ctime_diff") >= BREAK_DURATION || col("charge_state_change") =!= 0)
      .select("vin", "ctime")
      .toDF("vin", "end_time")
      .withColumn("end_time_pre", lag("end_time", 1, 0).over(w3))
      .toDF("vins","end_time","end_time_pre")
      //与当前运行时间范围的最大时间进行比较，判断当前行程是否完全结束（>Trip_end即为结束）
      .withColumn("trip_complete_flag",when(lit(time_max) - col("end_time") > TRIP_END_FLAG,1).otherwise(0) )

    //df_clean与df_end join,获取所有行程
    val trip_detail_df = df_clean
        .join(df_end, df_clean("vin") === df_end("vins")
          && df_clean("ctime") <= df_end("end_time")
          && df_clean("ctime") > df_end("end_time_pre"))
        .drop("vins")
        .drop("end_time_pre")

    //行程指标
    val trip_df = trip_detail_df
        .where("trip_complete_flag = 1 and cellVoltages is not null")
        .groupBy("vin", "charge_state", "end_time")
        .agg(min("ctime").as("start_time")
          ,avg("current").as("avg_curr")
          ,max("current").as("max_curr")
          ,min("current").as("min_curr")
          ,(max(struct("ctime","soc"))("soc") - min(struct("ctime","soc"))("soc")).as("soc_diff")
          ,max(struct("ctime","voltage"))("voltage").as("end_volt")
          ,(getArrayMax(max(struct("ctime","cellVoltages"))("cellVoltages")) - getArrayMin(max(struct("ctime","cellVoltages"))("cellVoltages"))).as("end_volt_diff")
          )
        .withColumn("duration", col("end_time") - col("start_time"))
        .withColumn("day_of_year", from_unixtime(col("start_time"), "yyyy-MM-dd"))
        .where("duration > 0")
        .select("vin", "charge_state","start_time", "end_time","duration","avg_curr","max_curr","min_curr","soc_diff","end_volt","end_volt_diff","day_of_year")

    trip_df.cache()

    writeTmpTbl(trip_detail_df)
    writeTripTbl(trip_df)
    writeTripClickhouseTbl(spark.table("%s.%s".format("source_gx", "dw_tripdivide")),properties)


  }
}
