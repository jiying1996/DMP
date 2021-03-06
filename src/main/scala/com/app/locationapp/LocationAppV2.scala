package com.app.locationapp

import com.utils.LocationUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object LocationAppV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合，存储输入输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet(inputPath)
    df.map(row => {
      //先去获取需要的参数：原始，有效，广告。。。
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      // 此时我们拿到了所有的数据，那么如何处理？
      // 写一个工具类，然后使用集合的方式进行处理。
      val reqList: List[Double] = LocationUtils.requestUtils(requestmode, processnode)
      val adList = LocationUtils.requestAD(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      val clickList = LocationUtils.requestShow(requestmode, iseffective)

      ((row.getAs[String]("provincename"), row.getAs[String]("cityname")),
        reqList ++ adList ++ clickList)
    })
      //    RDD[((String, String), List[Double])]
      .reduceByKey((list1, list2) => {
      //     List[(Double, Double)]
      list1.zip(list2).map(t => t._1 + t._2)
    }).map(t => t._1._1 + "," + t._1._2 + "," + t._2.mkString(","))
      .saveAsTextFile(outputPath)
  }
}
