package com.spark.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Logger, Level }

object sparkSqlLearning extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder().appName("Saprk SQL Learing").master("local[*]").getOrCreate()
  import spark.implicits._

  val df = spark.read.json("C:\\Users\\raokhan\\workspace\\SampleLearning\\input\\people.json")

  //df.printSchema()

  //df.select("name").show()

  //df.select($"name", ($"age"+1).alias("AGE_INCREMENT")).show()
  df.createOrReplaceTempView("Test")
  val newDF = spark.sql("select * from Test where age > 20")
  //newDF.show()

  val DF1 = df.withColumn("is_age_defined", when(col("age").isNull, lit(18)).otherwise(col("age")))
  // DF1.show()

  /* val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
  squaresDF.write.parquet("input/test_table/key=1")

  val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
  cubesDF.write.parquet("input/test_table/key=2")*/
  val cubesDF = spark.read.parquet("input/test_table/key=2")
  cubesDF.show()
  val mergedDF = spark.read.option("mergeSchema", "true").parquet("input/test_table")
  mergedDF.printSchema()
  mergedDF.show()

}