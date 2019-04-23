package com.spark.learning

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{ Logger, Level }
import org.codehaus.jackson.schema.JsonSchema


object JsonOperation extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("DataFrame Operations").master("local[*]").getOrCreate()
 
  val jsonDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq (
 (0, """{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }"""),
 (1, """{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }"""),
 (2, """{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }"""),
 (3, """{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }"""),
(4, """{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }"""),
(5, """{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }"""),
(6, """{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }"""),
(7, """{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }"""),
(8 ,""" {"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }"""),
(9,"""{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }""")))).toDF("id", "json")

//jsonDF.show()

val jsDF = jsonDF.select(col("id"), get_json_object(col("json"), "$.device_type").alias("device_type"),
                                          get_json_object(col("json"), "$.ip").alias("ip"),
                                         get_json_object(col("json"), "$.cca3").alias("cca3"))

                                       
                                         
 
}