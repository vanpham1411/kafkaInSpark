package org.example.wordCount

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode}

object wordCount {
  def main(args:Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val rdd = spark.sparkContext.textFile("./data/wordCountComplete/*")
    val cols = Seq("words")
    import spark.implicits._
    val df = rdd.toDF(cols:_*)
    val wordArray = df.select(functions.split(col("words")," ").as("words"))

    val wordCount = wordArray.select(explode(col("words")).as("word")).groupBy("word").count().as("count")

    wordCount.show()
//    val newDF = df.map(f=>{
//      val nameSplit = f.getAs[String](0).split(",")
//      (nameSplit(0),nameSplit(1))
//    })
//
//    val finalDF = newDF.toDF("word","cnt")
//    finalDF.printSchema()
//    finalDF.show()
//    finalDF.selectExpr("word","CAST(cnt as Integer)").groupBy("word").sum("cnt").as("count").show()


  }

}
