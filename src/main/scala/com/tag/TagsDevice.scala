package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagsDevice extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    // 获取设备操作系统类型
    val deviceOS = row.getAs[Int]("client")
    deviceOS match {
      case v if v == 1 || v == 2 || v == 3 => list :+= ("D0001000" + v, 1)
      case _ => list :+= ("D00010004", 1)
    }

    // 获取设备联网方式
    val netWork = row.getAs[String]("networkmannername")
    if (StringUtils.isNotBlank(netWork)) {
      netWork match {
        case v if StringUtils.equalsIgnoreCase("2G", v) => list :+= ("D00020004", 1)
        case v if StringUtils.equalsIgnoreCase("3G", v) => list :+= ("D00020003", 1)
        case v if StringUtils.equalsIgnoreCase("4G", v) => list :+= ("D00020002", 1)
        case v if StringUtils.equalsIgnoreCase("Wifi", v) => list :+= ("D00020001", 1)
        case _ => list :+= ("D00020005", 1)
      }
    }
    
    // 获取设备运营商联网方式
    val ispname = row.getAs[Int]("ispid")
    ispname match {
      case v if v == 1 || v == 2 || v == 3 => list :+= ("D0003000" + v, 1)
      case _ => list :+= ("D00030004", 1)
    }
    list
  }
}
