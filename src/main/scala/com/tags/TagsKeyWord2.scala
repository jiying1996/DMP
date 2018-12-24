package com.tags

import com.tag.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 关键字标签
  */
object TagsKeyWord2 extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 获取数据文件，然后进行处理
    val row = args(0).asInstanceOf[Row]
    val stopwords = args(1).asInstanceOf[Broadcast[Map[String, Int]]]

    // 首先处理数据
    row.getAs[String]("keywords").split("\\|")
      .filter(
      //进行数据的过滤
      word => word.length >= 3 && word.length <= 8 && 
        !stopwords.value.keySet.contains(word)
    ).foreach(word => list :+= ("K" + word, 1))

    list
  }
}
