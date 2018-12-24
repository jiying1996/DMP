package com.tag

import com.typesafe.config.ConfigFactory
import com.utils.{JedisConnectionPool, TagsUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 借助图计算中的连通图计算解决多个渠道用户身份识别
  */
object TagsContextV2 {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      println("目录不正确，退出程序！")
      sys.exit()
    }
    // 创建一个集合，存储输入输出目录
    val Array(inputPath, outputPath, dirPath, stopWords, day) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sqlContext.setConf("spark.io.compression.snappy.codec", "snappy")

    /**
      * Hbase连接
      */
    // 配置hbase连接
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.table.name")
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.host"))
    // 去加载我们的连接
    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbConn.getAdmin
    // 判断表是否存在
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      println("HBASE Table Name Create !!!")
      // 创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      // 创建一个列簇
      val columnDescriptor = new HColumnDescriptor("tags")
      // 将列簇加入到表中
      tableDescriptor.addFamily(columnDescriptor)
      hbadmin.createTable(tableDescriptor)
      // 关闭
      hbadmin.close()
      hbConn.close()
    }
    // 创建一个jobConf任务
    val jobConf = new JobConf(configuration)
    // 指定key的输出类型
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定输出到哪个表中
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)

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
    val baseRDD = sqlContext.read.parquet(inputPath)
      // 过滤数据ID
      .filter(TagsUtils.UserId)
      .map(row => {
        // 获取当前所有的用户非空ID
        val list = TagsUtils.getAnyAllUserId(row)
        (list, row)
      })
    // 构建点的集合 RDD[(List[String], Row)]
    val result = baseRDD.flatMap(tp => {
      // 取出row
      val row = tp._2
      // 标签处理
      val tagsAd: List[(String, Int)] = TagsAd.makeTags(row)
      val tagsAppname: List[(String, Int)] = TagsAppname.makeTags(row, broadcast)
      val tagsDevice: List[(String, Int)] = TagsDevice.makeTags(row)
      val tagsKeyWords: List[(String, Int)] = TagsKeyWords.makeTags(row, stopwords)
      val tagsLocation: List[(String, Int)] = TagsLocation.makeTags(row)
      // 商圈标签先不加，因为里面没数据
      //      val tagsBusiness = TagsBusiness.makeTags(row, jedis)
      val rowTag = tagsAd ++ tagsAppname ++ tagsDevice ++ tagsKeyWords ++ tagsLocation
      // 构建顶点
      val VD: List[(String, Int)] = tp._1.map((_, 0)) ++ rowTag
      // 只有第一行的第一个ID可以携带顶点ID，其他不要携带
      // 如果同一行上的有多个顶点就乱了，数据就会发生重复
      tp._1.map(uId => {
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
    })

    // 构建边集合
    val edges = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode.toLong, 0))
    })

    // 构建图
    val graph = Graph(result, edges)
    // 实现图连接
    val vertices = graph.connectedComponents().vertices
    // 认祖归宗
    val resRDD: RDD[(VertexId, List[(String, Int)])] = vertices.join(result).map {
      //    RDD[(VertexId, (VertexId, List[(String, Int)]))]
      case (uId, (commonId, tagsAndUserId)) => (commonId, tagsAndUserId)
    }.reduceByKey {
      case (list1, list2) => (list1 ++ list2)
        .groupBy(_._1)
        .mapValues(_.map(_._2).sum).toList
    }
    // 将结果存入到Hbase中
    resRDD.map {
      case (userid, userTags) => {
        val put = new Put(Bytes.toBytes(userid))
        val tags = userTags.map(t => t._1 + "," + t._2).mkString(",")
        // 将所有的数据输入到hbase
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$day"), Bytes.toBytes(tags))
        // HBASE 返回所有的数据
        (new ImmutableBytesWritable(), put)
      }
    }.saveAsHadoopDataset(jobConf)
    
    //      // 根据每一条数据需求进行打标签（6种）
    //      .mapPartitions(row => {
    //      // 创建一个Jedis
    //      val jedis = JedisConnectionPool.getConnection()
    //      var result = collection.mutable.ListBuffer[(String, List[(String, Int)])]()
    //      row.foreach(row => {
    //        // 首先先获取用户id
    //        val userid: List[String] = TagsUtils.getAnyAllUserId(row)
    //        // 开始整理标签
    //        val tagsAd: List[(String, Int)] = TagsAd.makeTags(row)
    //        val tagsAppname: List[(String, Int)] = TagsAppname.makeTags(row, broadcast)
    //        val tagsDevice: List[(String, Int)] = TagsDevice.makeTags(row)
    //        val tagsKeyWords: List[(String, Int)] = TagsKeyWords.makeTags(row, stopwords)
    //        val tagsLocation: List[(String, Int)] = TagsLocation.makeTags(row)
    //        val tagsBusiness = TagsBusiness.makeTags(row, jedis)
    //        result += ((userid, tagsAd ++ tagsAppname ++ tagsDevice ++ tagsKeyWords ++ tagsLocation ++ tagsBusiness))
    //      })
    //      // 关闭连接
    //      jedis.close()
    //      // 返回值
    //      result.iterator
    //    }).reduceByKey {
    //      (list1, list2) =>
    //        (list1 ++ list2).groupBy(_._1).map {
    //          case (key, list) => {
    //            (key, list.map(t => t._2).sum)
    //          }
    //        }.toList
    //      //        (list1 ::: list2)
    //      //          .groupBy(_._1)
    //      //          .mapValues(_.foldLeft[Int](0)(_ + _._2))
    //      //          .toList
    //
    //    }.map(t => {
    //      t._1 + "," + t._2.map(x => x._1 + "," + x._2).mkString(",")
    //    }).saveAsTextFile(outputPath)
    sc.stop
  }
}
