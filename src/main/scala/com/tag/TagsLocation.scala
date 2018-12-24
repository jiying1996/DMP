package com.tag

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object TagsLocation extends Tags {
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args(0).asInstanceOf[Row]

    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")
    
    if(StringUtils.isNotEmpty(provincename)){
      list :+= ("ZP" + provincename, 1)
    }
    if(StringUtils.isNotEmpty(provincename)){
      list :+= ("ZC" + cityname, 1)
    }

    list
  }
}
