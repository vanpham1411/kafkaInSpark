package org.example.customSerialize

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object writeToHdfs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user-topic")
      .option("failOnDataLoss","false")
      .option("startingOffsets", "earliest")
      //
      .option(ConsumerConfig.SEND_BUFFER_CONFIG,"1024")
      .option(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"5000")
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
      .option("path","hdfs://localhost:9000/newUserInfo")
      .option("checkpointLocation", "/tmp/checkpoint")
      //SET BUFFER CONFIG
      .option(ProducerConfig.BUFFER_MEMORY_CONFIG,64)
      //SET BATCH SIZE CONFIG
      .option(ProducerConfig.BATCH_SIZE_CONFIG,2048)
      .option("minOffsetsPerTrigger",5)
      //Xử lý sau mỗi 5 giây
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      .start()
      .awaitTermination()
  }

}
