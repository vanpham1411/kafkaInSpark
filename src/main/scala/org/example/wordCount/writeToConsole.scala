package org.example.wordCount

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode, window}

object writeToConsole {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","word-count")
      .option("failOnDataLoss","false")
      .load()
    df.printSchema()
    import spark.implicits._
    val valueDF = df.selectExpr("CAST(value as String)")

    val wordArray = valueDF.select(functions.split(col("value")," ").as("words"))

    val words = wordArray.select(explode(col("words")).as("word"))
    val value = words.groupBy("word")
      .count().as("count")

    val query = value.writeStream
            .format("console")
            .outputMode("complete")
            .option("truncate", "false")
            .start()
            .awaitTermination()



  }

}
