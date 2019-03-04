package com.parquet

import java.util.Properties

import akka.event.slf4j.Logger
import com.typesafe.config.ConfigFactory
import com.utils.{NBF, SchemaUtils}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 处理本地数据文件转换成 parquet（文件小，查询快）
  */
object Bz2Parquet {
  def main(args: Array[String]): Unit = {
    // 模拟企业开发模式，首先判断一下目录 是否为空
    if (args.length != 2) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    val logger = new Logger
    // 创建一个集合，存储输入输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName).setMaster("local")
      // 处理数据，采取scala的序列化方式，性能比Java默认的高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    // 我们要采取snappy压缩方式，因为咱们现在用的是1.6版本的spark，到2.0以后就是默认的，不需要配置
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val lines = sc.textFile("F:\\千锋学习\\8.Spark项目\\DMP\\2016-10-01_06_p1_invalid.1475274123982.log.FINISH.bz2")

    val rowRDD = lines.map(t => t.split(",", t.length))
      .filter(_.length >= 85).map(arr => {
      Row(
        arr(0),
        NBF.toInt(arr(1)),
        NBF.toInt(arr(2)),
        NBF.toInt(arr(3)),
        NBF.toInt(arr(4)),
        arr(5),
        arr(6),
        NBF.toInt(arr(7)),
        NBF.toInt(arr(8)),
        NBF.toDouble(arr(9)),
        NBF.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NBF.toInt(arr(17)),
        arr(18),
        arr(19),
        NBF.toInt(arr(20)),
        NBF.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NBF.toInt(arr(26)),
        arr(27),
        NBF.toInt(arr(28)),
        arr(29),
        NBF.toInt(arr(30)),
        NBF.toInt(arr(31)),
        NBF.toInt(arr(32)),
        arr(33),
        NBF.toInt(arr(34)),
        NBF.toInt(arr(35)),
        NBF.toInt(arr(36)),
        arr(37),
        NBF.toInt(arr(38)),
        NBF.toInt(arr(39)),
        NBF.toDouble(arr(40)),
        NBF.toDouble(arr(41)),
        NBF.toInt(arr(42)),
        arr(43),
        NBF.toDouble(arr(44)),
        NBF.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NBF.toInt(arr(57)),
        NBF.toDouble(arr(58)),
        NBF.toInt(arr(59)),
        NBF.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NBF.toInt(arr(73)),
        NBF.toDouble(arr(74)),
        NBF.toDouble(arr(75)),
        NBF.toDouble(arr(76)),
        NBF.toDouble(arr(77)),
        NBF.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NBF.toInt(arr(84))
      )
    })

    val df = sqlContext.createDataFrame(rowRDD, SchemaUtils.schema)

    df.write.parquet(outputPath)
    df.show()
    sc.stop()
  }
}
