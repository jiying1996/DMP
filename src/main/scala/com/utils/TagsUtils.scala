package com.utils

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 处理标签的工具类
  */
object TagsUtils {
  val UserId =
    """
      |imei != '' or mac != '' or idfa != '' or openudid != '' or androidid != '' or
      |imeimd5 != '' or macmd5 != '' or idfamd5 != '' or openudidmd5 != '' or androididmd5 != '' or
      |imeisha1 != '' or macsha1 != '' or idfasha1 != '' or openudidsha1 != '' or androididsha1 != ''
    """.stripMargin

  /**
    * 获取ID
    *
    * @param row
    * @return
    */
  def getAnyOneUserId(row: Row): String = {
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) =>
        "TM：" + v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) =>
        "MC：" + v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) =>
        "ID：" + v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) =>
        "OD：" + v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) =>
        "AOD：" + v.getAs[String]("androidid")

      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) =>
        "TMM：" + v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) =>
        "MCM：" + v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) =>
        "IDM：" + v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) =>
        "ODM：" + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) =>
        "AODM：" + v.getAs[String]("androididmd5")

      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) =>
        "TMS：" + v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) =>
        "MCS：" + v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) =>
        "IDS：" + v.getAs[String]("idfasha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) =>
        "ODS：" + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) =>
        "AODS：" + v.getAs[String]("androididsha1")
    }
  }

  // 获取所有的ID
  def getAnyAllUserId(row: Row): List[String] = {
    var list = List[String]()
    if (StringUtils.isNotEmpty(row.getAs[String]("imei"))) {
      list :+= "TM:" + row.getAs[String]("imei")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("mac"))) {
      list :+= "MC:" + row.getAs[String]("mac")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("idfa"))) {
      list :+= "ID:" + row.getAs[String]("idfa")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("androidid"))) {
      list :+= "AD:" + row.getAs[String]("androidid")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("openudid"))) {
      list :+= "OU:" + row.getAs[String]("openudid")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("imeimd5"))) {
      list :+= "TMM:" + row.getAs[String]("imeimd5")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("macmd5"))) {
      list :+= "MCM:" + row.getAs[String]("macmd5")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("idfamd5"))) {
      list :+= "IDM:" + row.getAs[String]("idfamd5")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("androididmd5"))) {
      list :+= "ADM:" + row.getAs[String]("androididmd5")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("openudidmd5"))) {
      list :+= "OUM:" + row.getAs[String]("openudidmd5")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("imeisha1"))) {
      list :+= "TMS:" + row.getAs[String]("imeisha1")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("macsha1"))) {
      list :+= "MCS:" + row.getAs[String]("macsha1")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("idfasha1"))) {
      list :+= "IDS:" + row.getAs[String]("idfasha1")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("androididsha1"))) {
      list :+= "ADS:" + row.getAs[String]("androididsha1")
    }
    if (StringUtils.isNotEmpty(row.getAs[String]("openudidsha1"))) {
      list :+= "OUS:" + row.getAs[String]("openudidsha1")
    }
    list
  }
}
