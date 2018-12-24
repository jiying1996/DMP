package com.tags

import com.tag.Tags
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object TagsLocation2 extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]
    // 取出对应的数据
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    // 判断取出的数据是否为空
    if (StringUtils.isNotBlank(provincename)) {
      list :+= ("ZP" + provincename, 1)
    }
    if (StringUtils.isNotBlank(cityname)) {
      list :+= ("ZC" + cityname, 1)
    }
    list
  }
}
