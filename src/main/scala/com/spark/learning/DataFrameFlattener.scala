package com.spark.learning

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{ Logger, Level }


object flattening extends App {

  implicit class DataFrameFlattenerTest(df: DataFrame) {
    def flattenSchema: DataFrame = {
      df.select(flatten(Nil, df.schema): _*)
    }

    def flatten(path: Seq[String], schema: DataType): Seq[Column] = schema match {
      case s: StructType => s.fields.flatMap(f => flatten(path :+ f.name, f.dataType))
      case other         => col(path.map(n => s"`$n`").mkString(".")).as(path.mkString(".")) :: Nil
    }
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("Flatten").master("local[*]").getOrCreate()

  val nestedDF = spark.sqlContext.createDataFrame(spark.sparkContext.parallelize(Seq(("1", (2, (3, 4)))))).toDF()
   nestedDF.show()
    nestedDF.printSchema()
  val colNames = List("A", "B", "C", "D")
   nestedDF.flattenSchema.toDF(colNames:_*).show()

  val udfDF = spark.read.textFile("C:\\Users\\raokhan\\workspace\\SampleLearning\\input\\input.txt")
   println(udfDF.count)

  val header = udfDF.first()
  val rdd_noheader = udfDF.filter(x => x != header)

   println(rdd_noheader.first())

  // val df_schema = StructType(header.split("|").map(fieldName => StructField(fieldName, StringType, true)))
  val df_schema = StructType(header.split('|').map(fieldName => StructField(fieldName, StringType, true)))

  val row_rdd = rdd_noheader.rdd.map(x => x.split('|')).map(x => Row.fromSeq(x))

  var df = spark.createDataFrame(row_rdd, df_schema)
   df.show()
    df.printSchema

  //Conver to date type
  val timeStamp2dataTypes: (Column) => Column = x => {
    to_date(x)
  }

  df = df.withColumn("DateType", timeStamp2dataTypes(col("end_date")))
   df.show()
  // parse date only
  val parseDate: (Column) => Column = x => {
    regexp_replace(x, " (\\d+)[:](\\d+)[:](\\d+).*$", "")
  }

  df = df.withColumn("Date_Only", parseDate(col("end_date")))
   df.show()

  val parse_city: (Column) => Column = (x) => { split(x, "-")(1) }
  df = df.withColumn("city", parse_city(col("location")))
   df.show()

  // Perform a date diff function
  val dateDiff: (Column, Column) => Column = (x, y) => { datediff(to_date(y), to_date(x)) }
  df = df.withColumn("date_diff", dateDiff(col("start_date"), col("end_date")))
  df.show()

 // df.printSchema()
 // val correlation = df.stat.corr("id", "date_diff")
 // println(correlation)
  
  // Create table or view and query them
  df.createOrReplaceTempView("NewTable")
  val sqlDF = spark.sql("""select * from NewTable""")
  sqlDF.show()

  //Creating Json format and writing to output folder
  val rdd_json = df.toJSON
  //rdd_json.write.json("C:\\Users\\raokhan\\workspace\\SampleLearning\\output")

  val add_n = udf((x:Integer,y:Integer) => x +y)
  df = df.withColumn("Added_N", add_n(col("id").cast("int"),lit(1000)))
  df.show()
  df.printSchema()
  
  val correlation  = df.stat.corr("date_diff", "Added_N")
  println(correlation)
  
  val dateExp = df.withColumn("newDate" ,date_format(current_date(), "yyyyMMdd"))  
  dateExp.show()
  
  
}