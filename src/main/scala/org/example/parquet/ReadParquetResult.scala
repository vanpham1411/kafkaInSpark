package org.example.parquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object ReadParquetResult {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .getOrCreate()
    val df = spark.read.parquet("hdfs://localhost:9000/inputParquetData")
    df.printSchema()
    val schema = new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType)
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
    val structDF = df.select(from_json(col("value"),schema).as("data"))
      .select("data.*")
    structDF.show(false)
  }

}
