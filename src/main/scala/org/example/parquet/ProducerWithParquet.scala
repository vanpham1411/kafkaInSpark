package org.example.parquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{struct, to_json}

object ProducerWithParquet {
  def main(args : Array[String]): Unit =  {
    val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df = spark.read.parquet("./data/inputParquet.parquet")
    val kafkaServer: String = "localhost:9092"
    val topicSampleName: String = "kafkatopic"

    df.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", topicSampleName)
      .save()
  }

}
