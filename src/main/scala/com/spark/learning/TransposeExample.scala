package com.spark.learning

import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransposeExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TransposeExample").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val df = spark.sqlContext.createDataFrame((sc.parallelize(Seq((1, 0.0, 0.6), (1, 0.6, 0.7))))).toDF("A", "col_1", "col_2")
    // df.show()
    
    //def toLong(df: DataFrame, by: Seq[String]): DataFrame = {
    val (cols, types) = df.dtypes.filter { case (c, _) => !Seq("A").contains(c) }.unzip
    require(types.distinct.size == 1, s"${types.distinct.toString}.length != 1")
    
    
      val kvs = explode(array(cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*))
      
       //println(explode(array(cols.map(c => struct(lit(c).alias("key"), col(c).alias("val"))): _*)))
 // kvs.alias("_kvs"): _*
      val byExprs = Seq("A").map(col(_))
     // df.select(byExprs, cols)

  df.select(byExprs :+ kvs.alias("_kvs"): _*).show()
  //.select(byExprs ++ Seq($"_kvs.key", $"_kvs.val"): _*)

//}

 //   toLong(df, Seq("A"))

  }
}