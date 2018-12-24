package com.tag

import ch.hsr.geohash.GeoHash
import com.utils.{BaiduLBSHandler, JedisConnectionPool}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 解析经纬度，并讲商圈信息存入Redis
  */
object LongAndLat2BaiDu {
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
    // 拿到数据的经纬度
    sqlContext.read.parquet(inputPath).select("lat", "long")
      .filter(
        """
          |cast(long as double) >= 73 and cast(long as double) <= 136 and
          |cast(lat as double) >= 3 and cast(lat as double) <= 54
        """.stripMargin).distinct()
      .foreachPartition(f => {
        val jedis = JedisConnectionPool.getConnection()
        f.foreach(f => {
          // 获取到过滤后的经纬度
          val long = f.getAs[String]("long")
          val lat = f.getAs[String]("lat")
          // 通过百度的逆地址解析 获取到商圈信息
          val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)

          // 进行SN校验 得到的是百度商圈
          val baiduSN = BaiduLBSHandler.parseBusinessTagBy(long, lat)
          jedis.set(geoHash, baiduSN)
        })
        jedis.close()
      })
    sc.stop()
  }
}