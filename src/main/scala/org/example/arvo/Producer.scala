package org.example.arvo

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions.{from_avro, to_avro}
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.nio.file.{Files, Paths}

object Producer {
  def main(args:Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .getOrCreate()

    //GET DATA FROM JSON FILE
    val df = spark.read
      .json("./data/avro/input/person.json")
    df.printSchema()
    df.show(false)
    println("")
    val valuedf = df.select(to_avro(struct("*")).as("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","avro_topic")
      .option("checkpointLocation","/tmp/checkpoint")
      .save()




  }

}
