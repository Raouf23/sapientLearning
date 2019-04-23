package com.spark.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}
import org.apache.spark.sql.functions.{col, udf}

object sparkExcel {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("ExcelExample")
      .getOrCreate()

    //val excelDF = spark.sqlContext.read.format("com.crealytics.spark.excel").option("location", "C:\\Users\\raokhan\\workspace\\SampleLearning\\input\\MOCK_DATA.xlsx").load()

    val excelDF = spark.read.format("com.crealytics.spark.excel")
      .option("location", "C:\\Users\\raokhan\\workspace\\SampleLearning\\input\\MOCK_DATA.xlsx")
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", "False")
      .load()

    excelDF.show(10)

    val maleDF = excelDF.filter(col("gender")=== "Male")
    maleDF.show(10)

  }
}