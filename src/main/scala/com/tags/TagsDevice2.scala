package com.tags

import com.tag.Tags
import org.apache.spark.sql.Row

/**
  * 设备标签
  */
object TagsDevice2 extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    // 操作系统
    val client = row.getAs[Int]("client")
    client match {
      case 1 => list :+= ("Android D00010001", 1)
      case 2 => list :+= ("IOS D00010002", 2)
      case 3 => list :+= ("WinPhone D00010003", 3)
      case _ => list :+= ("其他 D00010004", 4)
    }

    //设备联网符方式
    val network = row.getAs[String]("newtworkmannername")
    network match {
      case "Wifi" => list :+= ("WIFI D00020001", 1)
      case "4G" => list :+= ("4G D00020002", 2)
      case "3G" => list :+= ("3G D00020003", 3)
      case "2G" => list :+= ("2G D00020004", 4)
      case _ => list :+= ("其他 D00020005", 4)
    }

    // 移动运营商
    val ispid = row.getAs[String]("ispname")
    ispid match {
      case "移动" => list :+= ("移 动 D00030001", 1)
      case "联通" => list :+= ("联 通 D00030002", 1)
      case "电信" => list :+= ("电 信 D00030003", 1)
      case _ => list :+= ("其他 D00030004", 1)
    }
    list
  }
}
