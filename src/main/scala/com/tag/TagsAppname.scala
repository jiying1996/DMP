package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Row, SQLContext}

/**
  * 打appname标签
  */
object TagsAppname extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    val broadcast = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    var appname = row.getAs[String]("appname")
    //    val broadcast = row.getAs[Broadcast[Map[String, String]]]("broadcast")

    if (StringUtils.isBlank(appname)) {
      appname = broadcast.value.getOrElse(row.getAs[String]("appid"), "unknown")
    }
    list :+= ("APP " + appname, 1)

    list
  }
}
