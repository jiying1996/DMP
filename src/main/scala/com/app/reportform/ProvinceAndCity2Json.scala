package com.app.reportform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ProvinceAndCity2Json {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    val df = sqlContext.read.parquet("hdfs://mini1:8020/DMPText").coalesce(1)
    df.registerTempTable("t_parquet")
    // 实现以provincename和cityname分组，求count并输出成json 如果cityname为空则
    val sql = "select count(*) as ct, provincename, cityname from t_parquet group by provincename, cityname order by ct desc"
//    val sql = "select tt.ct, tt.provincename, tt.cityname from (select * from (select count(*) as ct, provincename, cityname from t_parquet group by provincename, cityname, ct) as ttt order by ct desc) as tt group by provincename, cityname"
    
    //    判断cityname为null赋默认值为“未知”，但是没有把cityname为''的赋默认值为“未知”
//    val sql = "select count(*) as ct, provincename, nvl(cityname,\"未知\") as cityname from t_parquet group by provincename,nvl(cityname,\"未知\")"
//    完整代码
//    val sql = "select count(*) as ct, provincename, case when cityname = '' or cityname is null then \"未知\" else cityname end as cityname from t_parquet group by provincename,case when cityname = '' or cityname is null then \"未知\" else cityname end"
//    sqlContext.sql("select * from t_parquet where cityname='未知'").show()
    
    sqlContext.sql(sql).write.mode("append").json("hdfs://mini1:8020/DMP/reportform/provinceandcity/tojson")
//    sqlContext.sql(sql).show()
  }
}
