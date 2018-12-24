package com.app.channelreport

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object ChannelReport {
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
    df.registerTempTable("t_channel")
    
    val result = sqlContext.sql(
      """
        |select
        |adplatformproviderid,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysrequest,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxrequest,
        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as cybidsuccees,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as shows,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clicks,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) as dapcost,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as dsppayment
        |from t_adplatformproviderid group by adplatformproviderid
      """.stripMargin)
    
    // 存入数据库
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    val url = load.getString("jdbc.url")
    //    val table = load.getString("jdbc.tbn")
    val table = "rpt_channel"
    result.write.mode(SaveMode.Append).jdbc(url, table, props)
    sc.stop()
  }
}
