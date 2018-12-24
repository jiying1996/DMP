package com.app.reportform

import java.io.FileWriter

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ProvinceAndCityByCore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("hdfs://mini1:8020/DMPText")

    val out = new FileWriter("E://test/DMP/reportform")
    //    val out = new FileWriter("E://test/DMP2")

    val value: RDD[((Any, Any), Int)] = df.rdd.map(x => {
      ((x(24), x(25)), 1)
    }).reduceByKey(_ + _).sortBy(_._2, false)

    for (item <- value.collect()) {
      out.write(item._2 + "\t" + item._1._1 + "\t" + item._1._2 + "\n")
    }

    out.flush()
    sc.stop()
  }
}
