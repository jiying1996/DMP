package com.app.media

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

object Media {
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
    df.registerTempTable("t_media")
    val rowRDD = sc.textFile("F://千锋学习/8.Spark项目/DMP/app_dict.txt").map(_.split("\t", -1))
      .filter(_.length >= 6).map(arr => {
      Row(arr(1), arr(4))
    })
    
    val schema = StructType(
      Seq(
        StructField("appname", StringType, false),
        StructField("appurl", StringType, false)
      )
    )

    val dfDic = sqlContext.createDataFrame(rowRDD, schema)
    dfDic.registerTempTable("t_dic")

    //    val result = sqlContext.sql(
    //      """
    //        |select
    //        |appname,
    //        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysrequest,
    //        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxrequest,
    //        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as adrequest,
    //        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as cybid,
    //        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as cybidsuccees,
    //        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as shows,
    //        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clicks,
    //        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) as dapcost,
    //        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as dsppayment
    //        |from t_media where appname != "其他" group by appname
    //        |union
    //        |select
    //        |t_dic.appname as appname,
    //        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysrequest,
    //        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxrequest,
    //        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as adrequest,
    //        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as cybid,
    //        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as cybidsuccees,
    //        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as shows,
    //        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clicks,
    //        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) as dapcost,
    //        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as dsppayment
    //        |from t_media
    //        |join t_dic on t_dic.appurl = t_media.appid
    //        |where t_media.appname = "其他" group by t_dic.appname
    //      """.stripMargin)

    val result = sqlContext.sql(
      """
        |select
        |appname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysrequest,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxrequest,
        |sum(case when requestmode = 1 and processnode = 3 then 1 else 0 end) as adrequest,
        |sum(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end) as cybid,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 and adorderid != 0 then 1 else 0 end) as cybidsuccees,
        |sum(case when requestmode = 2 and iseffective = 1 then 1 else 0 end) as shows,
        |sum(case when requestmode = 3 and iseffective = 1 then 1 else 0 end) as clicks,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end) as dapcost,
        |sum(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end) as dsppayment
        |from t_media where appname != "其他" group by appname
      """.stripMargin)
    // 存入数据库
    val load = ConfigFactory.load()
    val props = new Properties()
    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))

    val url = load.getString("jdbc.url")
    //    val table = load.getString("jdbc.tbn")
    val table = "rpt_media2"
    result.write.mode(SaveMode.Append).jdbc(url, table, props)
    sc.stop()
  }
}
