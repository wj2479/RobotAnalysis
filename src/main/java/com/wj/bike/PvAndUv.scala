package com.wj.bike

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object PvAndUv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
      .set("spark.mongodb.input.uri", "mongodb://guest:123568@192.168.1.102:27017/gxbike.logs")
      .set("spark.mongodb.output.uri", "mongodb://guest:123568@192.168.1.102:27017/gxbike.result")
    val sc = new SparkContext(conf)

    val mongoRdd: MongoRDD[Document] = MongoSpark.load(sc)

    val pv = mongoRdd.count()

    val uv = mongoRdd.map(doc => {
      doc.get("openid")
    }).distinct().count()

    println(s"pv: $pv , uv:$uv")

    sc.stop()
  }
}
