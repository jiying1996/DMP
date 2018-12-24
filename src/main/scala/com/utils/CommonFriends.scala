package com.utils

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 图计算一个简单案例
  */
object CommonFriends {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("C").setMaster("local")
    val sc = new SparkContext(conf)

    // 构造出点的集合
    val rdd: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("梅西", 31)),
      (2L, ("阿奎罗", 32)),
      (6L, ("库蒂尼奥", 28)),
      (9L, ("席尔瓦", 29)),
      (133L, ("苏亚雷斯", 28)),
      (138L, ("奥尼尔", 40)),
      (16L, ("哈登", 29)),
      (21L, ("詹姆斯", 34)),
      (44L, ("罗斯", 30)),
      (158L, ("马化腾", 45)),
      (7L, ("马云", 54)),
      (5L, ("王健林", 55))
    ))

    // 构建边集合
    val rdd2: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))

    // 调用图计算方法 返回图
    val graph: Graph[(String, Int), Int] = Graph(rdd, rdd2)
    // 已经都连接到了顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //    vertices.foreach(println)
    // 进行简单操作
    vertices.join(rdd).map {
      case (userid, (cmId, (name, age))) => (cmId, List((name, age)))
    }.reduceByKey(_ ++ _).foreach(println)
    //    (1,List((詹姆斯,34), (哈登,29), (奥尼尔,40), (苏亚雷斯,28), 
    //    (梅西,31), (库蒂尼奥,28), (席尔瓦,29), (罗斯,30), (阿奎罗,32)))
    //    (5,List((马化腾,45), (马云,54), (王健林,55)))
    
  }
}
