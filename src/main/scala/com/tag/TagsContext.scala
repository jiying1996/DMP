package com.tag

import com.tags.TagsKeyWord2
import com.utils.TagsUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 创建上下文标签，用于合并所有标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合，存储输入输出目录
    val Array(inputPath, outputPath, dirPath, stopWords) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    // 读取字典文件
    val dirMap = sc.textFile(dirPath).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => {
        (arr(4), arr(1))
      }).collect().toMap

    // 广播字典文件
    val broadcast: Broadcast[Map[String, String]] = sc.broadcast(dirMap)
    // 读取停用词库
    val stopWordDir = sc.textFile(stopWords).map((_, 0)).collect().toMap
    // 广播字典文件
    val stopwords: Broadcast[Map[String, Int]] = sc.broadcast(stopWordDir)
    // 读取文件数据
    sqlContext.read.parquet(inputPath)
      // 过滤数据ID
      .filter(TagsUtils.UserId)
      // 根据每一条数据需求进行打标签（6种）
      .map(row => {
      // 首先先获取用户id
      val userid: String = TagsUtils.getAnyOneUserId(row)
      // 开始整理标签
      val tagsAd: List[(String, Int)] = TagsAd.makeTags(row)
      val tagsAppname: List[(String, Int)] = TagsAppname.makeTags(row, broadcast)
      val tagsDevice: List[(String, Int)] = TagsDevice.makeTags(row)
      val tagsKeyWords: List[(String, Int)] = TagsKeyWords.makeTags(row, stopwords)
      val tagsLocation: List[(String, Int)] = TagsLocation.makeTags(row)
      (userid, tagsAd ++ tagsAppname ++ tagsDevice ++ tagsKeyWords ++ tagsLocation)
    }).reduceByKey {
      (list1, list2) =>
        (list1 ++ list2).groupBy(_._1).map {
          case (key, list) => {
            (key, list.map(t => t._2).sum)
          }
        }.toList
//        (list1 ::: list2)
//          .groupBy(_._1)
//          .mapValues(_.foldLeft[Int](0)(_ + _._2))
//          .toList

    }.map(t => {
      t._1 + "," + t._2.map(x => x._1 + "," + x._2).mkString(",")
    }).saveAsTextFile(outputPath)
  }
}
