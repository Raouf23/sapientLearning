package com.spark.learning

import org.apache.spark.{ SparkContext, SparkConf, Partitioner }
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.sql.SparkSession

object ReduceAndFold {

  def main(args: Array[String]): Unit = {

     Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("ReduceAndFold").master("local[*]").getOrCreate()

    val sc = spark.sparkContext

    val input = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

    println("===================MapPartitions===================")

    input.mapPartitions(mapPartition).collect()

    println("********************** reduce *******************")
    
    println("input.reduce " + input.reduce((x, y) => add(x, y)))
    
    println("********************** fold *******************")
    
    println("input.fold " + input.fold(10)((x, y) => add(x, y)))
  }

  def add(x: Int, y: Int): Int = {
    println(s"Inside add -> $x, $y")
    x + y
  }

  def mapPartition(iterator: Iterator[Int]): Iterator[Int] = {
    println("======Inside mapPartition===============")
    iterator.foreach(println)
    iterator
  }
}