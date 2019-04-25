package com.spark.learning

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.functions._


object PivotExample extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().appName("PivotExample").master("local[*]").getOrCreate()
  
  val df  = spark.read.option("header", "true").
            option("inferschema","true").
            format("csv").
            csv("C:\\Users\\raokhan\\Downloads\\vehicles\\vehicles.csv")
            
  // df.show(20 ,false)
            
  val carDF = 
    df.select("year", "comb08","VClass").filter(col("VClass").contains("Cars"))
  .filter((col("year") === 2015 || col("year") === 2016 || col("year") === 2017 || col("year") === 2018))
  .withColumnRenamed("comb08", "mpg")
  .withColumnRenamed("VClass", "class")
  
  //val carDfGrouped = carDF.groupBy("year", "class").agg(avg(carDF.col("mpg"))).sort("class","year")
  //carDfGrouped.show()
  val pivotData = carDF.groupBy("class").pivot("year").agg(round(avg("mpg"),2)).sort("class")
  pivotData.show()
  
}