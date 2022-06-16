package org.example.customSerialize

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user-topic")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()
    val valueDF = df.selectExpr("CAST(value as String)")
    val schema = new StructType()
      .add("name",StringType)
      .add("age",IntegerType)
    val personDF = valueDF.select(from_json(col("value"), schema).as("data"))
      .select("data.*")
    personDF.printSchema()
    personDF.writeStream
      .format("csv")
      .option("path","./data/userInfo")
      .option("checkpointLocation", "/tmp/checkpoint")
      .option("truncate", "false")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

}
