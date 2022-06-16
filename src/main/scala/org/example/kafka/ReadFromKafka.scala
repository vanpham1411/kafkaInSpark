package org.example.kafka

import org.apache.spark.sql.SparkSession

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "text_topic")
      .option("failOnDataLoss","false")
      .option("startingOffsets", "latest")
      .load()
    df.printSchema()
    val valueDF = df.selectExpr("CAST(value as String)",
      "CAST(offset as String)","CAST(key as String)")
    valueDF.writeStream
      .format("csv")
      .option("path","./data/ArrayValue")
      .option("checkpointLocation", "/tmp/checkpoint")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

}
