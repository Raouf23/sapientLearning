package com.spark.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Logger, Level }

object WindowOperation extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("DataFrame Operations").master("local[*]").getOrCreate()

  val customer1 = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(List(
    ("Alice", "2016-05-01", 50.00),
    ("Alice", "2016-05-03", 45.00),
    ("Alice", "2016-05-04", 55.00),
    ("Bob", "2016-05-01", 25.00),
    ("Bob", "2016-05-04", 29.00),
    ("Bob", "2016-05-06", 27.00)))).toDF("name", "date", "amountSpent")

  // customer1.show()

  val winSpec = Window.partitionBy("name").orderBy("date").rowsBetween(-1, 1)
  val movingAvg = customer1.withColumn("MovingAvg", avg(customer1("amountSpent")).over(winSpec))
  println("================= MOVING AVG========================")
  movingAvg.show()

  val winSpec2 = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue, 0)
  val cumSum = customer1.withColumn("CumSum", sum(customer1("amountSpent")).over(winSpec2))
  println("=======================Cumulative SUM==================")
  cumSum.show()

  val wSpec3 = Window.partitionBy("name").orderBy("date")
  val lagData = customer1.withColumn("prevAmountSpent", lag(customer1("amountSpent"), 1).over(wSpec3))
  val leadData = customer1.withColumn("nextAmountSpent", lead(customer1("amountSpent"), 1).over(wSpec3))
  println("================LAG DATA======================")
  lagData.show
  println("==========================LEAD DATA======================")
  leadData.show()

  val rankData = customer1.withColumn("rank", rank().over(wSpec3))
  println("=========================RANK DATA============================")
  rankData.show()

  //Describe function is Spark sql to get all stat value
  val customer1Stat = customer1.describe()
  customer1Stat.show()

  val wSpec4 = Window.partitionBy("name").orderBy("amountSpent")
  val rankData1 = customer1.withColumn("rank", rank().over(wSpec4))

  //rankData1.show()
}