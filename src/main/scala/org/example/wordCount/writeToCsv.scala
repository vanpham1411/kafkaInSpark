package org.example.wordCount

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, explode, lead, window}

object writeToCsv {
  def main(args: Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val path = "./data/wordCountComplete"

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","word-count")
      .option("failOnDataLoss","false")
      .load()
    df.printSchema()
    import spark.implicits._
    val valueDF = df.selectExpr("CAST(value as String)", "CAST(timeStamp as Timestamp)")

    val wordArray = valueDF.select(functions.split(col("value")," ").as("words"),col("timeStamp"))

    val words = wordArray.select(explode(col("words")).as("word"),col("timeStamp"))

    val value = words
      .withWatermark("timeStamp", "2 minutes")
      .groupBy(
        window($"timeStamp", "4 minutes", "2 minute"),
        $"word")
      .count()



    val query = value.select("word","count").writeStream
          .format("csv")
          .option("path",path)
          .option("checkpointLocation", "/tmp/checkpoint")
          .outputMode("append")
          .start()
          .awaitTermination()

  }

}
