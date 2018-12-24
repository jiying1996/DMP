package com.exam

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LoginTime {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("C").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("E://test/JsonTest02.json")
    
    //    根据这个json文件统计手机用户用不同系统登录的次数
    df.registerTempTable("t_json")
    val sql = "select terminal, count(*) as times from t_json group by terminal"
    sqlContext.sql(sql).show()

    val sql2 = "select province, count(*) as times from t_json group by province order by times desc limit(2)"
    sqlContext.sql(sql2).show()

    val sql3 = "select sum(money) + sum(amount) as totalMoney from t_json where status = 1"
    sqlContext.sql(sql3).show()
    
    sc.stop()
  }
}
