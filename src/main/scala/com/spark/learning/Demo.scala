package com.spark.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

object Demo {
   def main(args: Array[String]): Unit = {
    println("Hello World")
    
      val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
    /*  // Load our input data.
      val input =  sc.textFile("C:\\Users\\raokhan\\workspace\\SampleLearning\\input\\wordCount.txt")
      // Split up into words.
      val words = input.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
     println(counts.first())*/
     
     val sqlContext = new SQLContext(sc)
     
     val left  = sqlContext.createDataFrame(sc.parallelize(List(
      Foo(1,"One")
    )))
    
    println("Left show")
    // left.show
      val right = sqlContext.createDataFrame(sc.parallelize(List(
      Foo(1,"Uno")
    )))
    println("Right show")
   // right.show
    
     val join1 = left.join(right, left.col("id").equalTo(right.col("id")))
   // join1.show()
    
     val join2 = left.as("left").join(right.as("right"), left.col("id").equalTo(right.col("id")))
    join1.show()
    join1.select(join2.col("left.name")).show()
    
  }
}