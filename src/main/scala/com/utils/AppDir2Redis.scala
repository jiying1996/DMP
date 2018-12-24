package com.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 处理字典文件数据，将其存储redis中
  */
object AppDir2Redis {
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
    // 读取字典文件数据
    val file = sc.textFile(inputPath)
    file.map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1)))
      // 调用foreachPartition，主要是为了减少创建jedis的次数
      // 此时创建jedis的次数和partition数量相等
      .foreachPartition(ite => {
      // 创建连接
      val jedis = JedisConnectionPool.getConnection()
      ite.foreach(f => {
        jedis.set(f._1, f._2)
      })
      jedis.close()
    })
    sc.stop()
  }
}
