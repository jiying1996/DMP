package com.exam

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LoginTimeCore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("C").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("E://test/JsonTest02.json")

    // 1
    df.rdd.map(t => {
      (t.getAs[String]("terminal"), 1)
    }).reduceByKey(_ + _).foreach(println)

    // 2
    df.rdd.map(t => {
      (t.getAs[String]("province"), 1)
    }).reduceByKey(_ + _).sortBy(_._2, false).take(2).foreach(println)
    // 3
    val money = df.rdd.filter(t => {
      StringUtils.equals(t.getAs[String]("status"), "1")
    }).map(t => {
      if (StringUtils.isNotBlank(t.getAs[String]("money"))) {
        t.getAs[String]("money").toInt
      } else if (StringUtils.isNotBlank(t.getAs[String]("amount"))) {
        t.getAs[String]("amount").toInt
      } else 0
    }).reduce(_ + _)

    print(money)
    sc.stop()
  }
}

case class Log(openid: String, phoneNum: String, moneyOrAmount: Int, date: String, lat: Double,
               log: Double, province: String, city: String, district: String, terminal: String, status: String)