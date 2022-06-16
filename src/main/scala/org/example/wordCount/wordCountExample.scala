package org.example.wordCount

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{StringType, StructType}

object wordCountExample {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val data = Seq("this is a sentence","another sentence"," this is another sentence")

//    val schema = new StructType()
//      .add("words",StringType)
    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF("words")
    val newDF = df.select(functions.split(col("words")," ").as("words"))
    newDF.printSchema()
    newDF.show(false)

    val words = newDF.select(explode(col("words")).as("word")).groupBy("word").count().as("count")
    words.printSchema()
    words.show(false)

  }

}
