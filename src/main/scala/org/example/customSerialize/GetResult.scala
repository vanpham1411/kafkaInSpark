package org.example.customSerialize

import org.apache.spark.sql.SparkSession

object GetResult {
  def main(args:Array[String]) : Unit = {
    val spark = SparkSession.builder()
      .master("local[4]")
      .getOrCreate()

    val df = spark.read.csv("hdfs://localhost:9000/newUserInfo").toDF("name","age")
    df.printSchema()
    df.show(false)
  }

}
