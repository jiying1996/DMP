package com.tag

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsBusiness extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    var list = List[(String, Int)]()
    val long = row.getAs[String]("long").toDouble
    val lat = row.getAs[String]("lat").toDouble
    // 取出经纬度
    if (long >= 73 && long <= 136 && lat >= 3 && lat <= 54) {
      // 取到geoHash 8位geoHash的字符个数
      val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
      // 进行取值
      val geo = jedis.get(geoHash)
      if (StringUtils.isNotBlank(geo)) {
        // 获取对应的商圈
        geo.split(";").foreach(t => list :+= (t, 1))
      }
    }
    list
  }
}
