package com.wj.bike

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object PvAndUvSQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(getClass.getSimpleName).master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://guest:123568@192.168.1.102:27017/gxbike.logs")
      .config("spark.mongodb.output.uri", "mongodb://guest:123568@192.168.1.102:27017/gxbike.result")
      .getOrCreate()

    val mongoDf: DataFrame = MongoSpark.load(spark)

    mongoDf.createTempView("v_logs")

    val result = spark.sql("select count(*) pv , count (distinct openid) uv from v_logs")
    result.show()

    //    println(s"pv: $pv , uv:$uv")
    MongoSpark.save(result)

    spark.stop()
  }
}
