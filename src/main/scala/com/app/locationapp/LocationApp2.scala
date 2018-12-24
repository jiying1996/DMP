package com.app.locationapp

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LocationApp2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合，存储输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet(inputPath)

    val keys: RDD[(Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String)] = df.rdd.map(x => {
      //先去获取需要的参数：原始，有效，广告。。。
      val requestmode: Int = x.getAs[Int]("requestmode")
      val processnode: Int = x.getAs[Int]("processnode")
      val iseffective: Int = x.getAs[Int]("iseffective")
      val isbilling: Int = x.getAs[Int]("isbilling")
      val isbid: Int = x.getAs[Int]("isbid")
      val iswin: Int = x.getAs[Int]("iswin")
      val adorderid: Int = x.getAs[Int]("adorderid")
      val winprice: Double = x.getAs[Double]("winprice")
      val adpayment: Double = x.getAs[Double]("adpayment")
      val provincename = x.getAs[String]("provincename")
      val cityname = x.getAs[String]("cityname")
      (requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment, provincename, cityname)
    })
    val resRDD: RDD[(Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String)] = keys.map(x => {
      val key = getRes(x)
      (key._1.toInt,
        key._2.toInt,
        key._3.toInt,
        key._4.toInt,
        key._5.toInt,
        key._6.toInt,
        key._7.toInt,
        key._8.toDouble,
        key._9.toDouble,
        key._10,
        key._11)
    })

    val res: RDD[(String, String, Int, Int, Int, Int, Int, Int, Int, Double, Double)] = resRDD.groupBy(x => {
      (x._10, x._11)
    }).map(x => {
      val key = x._1
      var r1 = 0
      var r2 = 0
      var r3 = 0
      var r4 = 0
      var r5 = 0
      var r6 = 0
      var r7 = 0
      var r8 = 0.0
      var r9 = 0.0
      for (elem <- x._2) {
        r1 += elem._1
        r2 += elem._2
        r3 += elem._3
        r4 += elem._4
        r5 += elem._5
        r6 += elem._6
        r7 += elem._7
        r8 += elem._8
        r9 += elem._9
      }
      (key._1, key._2, r1, r2, r3, r4, r5, r6, r7, r8, r9)
    })
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    val url = load.getString("jdbc.url")
    val table = "rpt_ll2"
    import sqlContext.implicits._
    res.toDF("provincename", "cityname", "ysrequest", "yxrequest", "adrequest", "cybid", "cybidsuccees", "shows", "clicks", "dapcost", "dsppayment")
      .write.jdbc(url, table, props)

    //    res.saveAsTextFile("E://test/DMP/location")
    sc.stop()
  }

  def getRes(x: (Int, Int, Int, Int, Int, Int, Int, Double, Double, String, String)) = {
    var ysrequest = 0
    var yxrequest = 0
    var adrequest = 0
    var cybid = 0
    var cybidsuccees = 0
    var shows = 0
    var clicks = 0
    var dapcost = 0.0
    var dsppayment = 0.0
    if (x._1 == 1 && x._2 >= 1) {
      ysrequest = 1
    }
    if (x._1 == 1 && x._2 >= 2) {
      yxrequest = 1
    }
    if (x._1 == 1 && x._2 == 3) {
      adrequest = 1
    }
    if (x._3 == 1 && x._4 == 1 && x._5 == 1) {
      cybid = 1
    }
    if (x._3 == 1 && x._4 == 1 && x._6 == 1 && x._7 != 0) {
      cybidsuccees = 1
    }
    if (x._1 == 2 && x._3 == 1) {
      shows = 1
    }
    if (x._1 == 3 && x._3 == 1) {
      clicks = 1
    }
    if (x._3 == 1 && x._4 == 1 && x._6 == 1) {
      dapcost = x._8 / 1000
      dsppayment = x._9 / 1000
    }
    (ysrequest, yxrequest, adrequest, cybid, cybidsuccees, shows, clicks, dapcost, dsppayment, x._10, x._11)
  }
}

