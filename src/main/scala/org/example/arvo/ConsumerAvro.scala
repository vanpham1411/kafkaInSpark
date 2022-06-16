package org.example.arvo

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.{col, struct}

import java.nio.file.{Files, Paths}

object ConsumerAvro {
  def main(args:Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","avro_topic")
      .option("startingOffsets","latest")
      .load()
    println("get schema of data frame")
    df.printSchema()

    val jsonFormatSchema = new String(
      Files.readAllBytes(Paths.get("data/avro/input/person.avsc")))
    val valueDF = df.select(from_avro(col("value"),jsonFormatSchema).as("person"))
      .select("person.*")
    valueDF.printSchema()
    valueDF.writeStream
      .format("console")
      .outputMode("append")
      .option("mode","PERMISSIVE")
      .start()
      .awaitTermination()
  }

}
