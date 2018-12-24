package com.tags

import com.tag.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * APP标签
  */
object TagsAPP2 extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 获取Row类型的数据
    val row = args(0).asInstanceOf[Row]
    // 获取我们字典文件
    val appdir = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    // 获取appname、appid
    var appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if (StringUtils.isBlank(appname) && StringUtils.isNotBlank(appid)) {
      list :+= ("APP" + appdir.value.getOrElse(appid, "未知"), 1)
    } else {
      list :+= ("APP" + appname, 1)
    }
    list

  }
}
