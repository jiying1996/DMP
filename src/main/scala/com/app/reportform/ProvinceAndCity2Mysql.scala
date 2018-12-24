package com.app.reportform

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * 根据指标需求，将数据写入mysql
  */
object ProvinceAndCity2Mysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("hdfs://mini1:8020/DMPText")
    df.registerTempTable("t_parquet")

    val sql = "select count(*) as ct, provincename, cityname from t_parquet group by provincename,cityname"

    // 将数据存储到 mysql中，需要加载application.conf文件中的配置项
    val load = ConfigFactory.load()
    val props = new Properties()

    props.setProperty("user", load.getString("jdbc.user"))
    props.setProperty("password", load.getString("jdbc.password"))
    //    props.put("driver", "com.mysql.jdbc.Driver")
    val url = load.getString("jdbc.url")
    val table = load.getString("jdbc.tableName")

    sqlContext.sql(sql).write.mode(SaveMode.Append).jdbc(url, table, props)

    sc.stop()
  }
}
