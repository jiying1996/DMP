package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告标签
  */
object TagsAd extends Tags {

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    // 获取广告的参数
    val adType = row.getAs[Int]("adspacetype")
    // 广告位类型标签
    adType match {
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v > 0 && v <= 9 => list :+= ("LC0" + v, 1)
    }
    
    // 广告位类型名称标签
    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)) {
      list :+= ("LN" + adName, 1)
    }
    
    // 渠道标签
    val channel = row.getAs[Int]("adplatformproviderid")
    list :+= ("CN" + channel, 1)
 
    list
  }
}
