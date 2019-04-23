package com.spark.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Logger, Level }

object WindowFunction extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("DataFrame Operations").master("local[*]").getOrCreate()

  val customer1 = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(List(
    ("Alice", "2016-05-01", 50.00),
    ("Alice", "2016-05-03", 45.00),
    ("Alice", "2016-05-04", 55.00),
    ("Bob", "2016-05-01", 25.00),
    ("Bob", "2016-05-04", 29.00),
    ("Bob", "2016-05-06", 27.00)))).toDF("name", "date", "amountSpent")

  /**
   * Adding Static columns on DataFrame
   */
  val df2 = customer1
    .withColumn("Tasty", lit(true))
    .withColumn("correlation", lit(1))
    .withColumn("Stock Min Max", typedLit(Seq(100, 500)))

  df2.show
  /**
   * Get columns name for data frame
   */
  val columns = df2.columns
  columns.foreach(println _)
  /**
   * Get columns name and data types of columns
   */
  val datatypes = df2.dtypes
  datatypes.foreach(println _)

  val donuts = Seq(("111", "plain donut", 1.50), ("222", "vanilla donut", 2.0), ("333", "glazed donut", 2.50))

  val inventory = Seq(("111", 10), ("222", 20), ("333", 30))

  val dfDonuts = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(donuts)).toDF("Id", "Donut Name", "Price")
  dfDonuts.show()
  val dfInventory = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(inventory)).toDF("Id", "Inventory")
  dfInventory.show()

  val dfDonutInventory = dfDonuts.join(dfInventory, Seq("Id"), "inner")
  dfDonutInventory.show()
  dfDonutInventory.printSchema()

  val splitDF = df2.select(col("Tasty"), col("Stock Min Max")(0) as "MinStock", col("Stock Min Max")(1) as "MaxStock",lit("INDIA").alias("COUNTRY"))
   splitDF.show()

  val stockMinMax: (String => Seq[Int]) = (donutName: String) => donutName match {
    case "plain donut"   => Seq(100, 200)
    case "vanilla donut" => Seq(200, 300)
    case "glazed donut"  => Seq(300, 400)
    case _               => Seq(0, 0)
  }

  /**
   * UDF example in DataFrames
   */
  val udfStockMinMax = udf(stockMinMax)
  val donutStockDF = dfDonuts.withColumn("Stock Min Max", udfStockMinMax(col("Donut Name")))
  val firstRowColumn1 = donutStockDF.first().get(0)
  println(s"First row column 1 = colfirstRowColumn1")
  donutStockDF.show()
   println(firstRowColumn1)
  val dateDonut = Seq(
    ("plain donut", 1.50, "2018-04-17"),
    ("vanilla donut", 2.0, "2018-04-01"), ("glazed donut", 2.50, "2018-04-02"))

  val dateDonutDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(dateDonut))
    .toDF("Donut Name", "Price", "Purchase Date")

   dateDonutDF.show()

  /**
   * Formatting Examples in DataFrames
   */
  val formattedDF = dateDonutDF.withColumn("Price_Formatted", format_number(col("Price"), 2))
    .withColumn("Name_Formatted", format_string("awesome %s", col("Donut Name")))
    .withColumn("Name_Upper", upper(col("Donut Name")))
    .withColumn("Name_Lower", lower(col("Donut Name")))
    .withColumn("Date_Formatted", date_format(col("Purchase Date"), "yyyyMMdd"))
    .withColumn("Day", dayofmonth(col("Purchase Date")))
    .withColumn("Month", month(col("Purchase Date")))
    .withColumn("Year", year(col("Purchase Date")))

   formattedDF.show(false)

  /**
   * Hash Function in DataFrames
   */
  dateDonutDF
    .withColumn("Hash", hash(col("Donut Name"))) // murmur3 hash as default.
    .withColumn("MD5", md5(col("Donut Name")))
    .withColumn("SHA1", sha1(col("Donut Name")))
    .withColumn("SHA2", sha2(col("Donut Name"), 256)) // 256 is the number of bits
  .show(false)

  /**
   * String Function in DataFrame
   */
  dateDonutDF.withColumn("Contains plain", instr(col("Donut Name"), "donut"))
    .withColumn("Length", length(col("Donut Name")))
    .withColumn("Trim", trim(col("Donut Name")))
    .withColumn("LTrim", ltrim(col("Donut Name")))
    .withColumn("RTrim", rtrim(col("Donut Name")))
    .withColumn("Reverse", reverse(col("Donut Name")))
    .withColumn("Substring", substring(col("Donut Name"), 0, 5))
    .withColumn("IsNull", isnull(col("Donut Name")))
    .withColumn("Concat", concat_ws(" - ", col("Donut Name"), col("Price")))
    .withColumn("InitCap", initcap(col("Donut Name")))
   .show()

  val donutNull = Seq(("plain donut", 1.50), (null, 2.0), ("glazed donut", 2.50))
  val donutNullDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(donutNull))
    .toDF("Donut Name", "Price")

  donutNullDF.show()

  val nullDFDrop = donutNullDF.na.drop()
  nullDFDrop.show()

  val nullDFFill = donutNullDF.na.fill("--")
  nullDFFill.show()
}