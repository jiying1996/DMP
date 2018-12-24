package com.tag

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 打关键字标签
  */
object TagsKeyWords extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    val stopwords: Broadcast[Map[String, Int]] = args(1).asInstanceOf[Broadcast[Map[String, Int]]]
    val stopwordsSet: Set[String] = stopwords.value.keySet

    val keywords: Array[String] = row.getAs[String]("keywords").split("\\|")
    for (elem <- keywords) {
      if (elem.length <= 8 && elem.length >= 3 && !stopwordsSet.contains(elem)) {
        list :+= ("K" + elem, 1)
      }
    }

    list
  }
}
