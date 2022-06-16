package org.example.parquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object readFromKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .getOrCreate()
    import spark.implicits._
    val df = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "kafkatopic")
      .load()
    val valueDF =  df.selectExpr( "CAST(value AS STRING)")
    valueDF.printSchema()


    valueDF.write
        .parquet("hdfs://localhost:9000/inputParquetData")

  }

}
