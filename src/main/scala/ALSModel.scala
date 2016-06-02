package org.apache.spark.mllib.recommendation
// This must be the same package as Spark's MatrixFactorizationModel because
// MatrixFactorizationModel's constructor is private and we are using
// its constructor in order to save and load the model

import org.template.serendipitous.ALSAlgorithmParams
import org.template.serendipitous._

import io.prediction.controller.IPersistentModel
import io.prediction.controller.IPersistentModelLoader
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._

import grizzled.slf4j.Logger

import org.jblas.DoubleMatrix


class ALSModel(
    override val rank: Int,
    override val userFeatures: RDD[(Int, Array[Double])],
    override val productFeatures: RDD[(Int, Array[Double])],
    val productModels: Map[Int, ProductModel],
    val userStringIntMap: BiMap[String, Int],
    val itemStringIntMap: BiMap[String, Int],
    val items: Map[Int, Item],
    val preparedRecs: Map[String,Array[ItemScore]])
    //val userHistories: RDD[(String,Array[Int])])
    //val graph: RDD[Graph[String,Double]])
  extends MatrixFactorizationModel(rank, userFeatures, productFeatures)
  with IPersistentModel[ALSAlgorithmParams] {

@transient lazy val logger = Logger[this.type]

  def save(id: String, params: ALSAlgorithmParams,
    sc: SparkContext): Boolean = {

    sc.parallelize(Seq(rank)).saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/rank")
    userFeatures.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/userFeatures")
    productFeatures.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/productFeatures")
    sc.parallelize(Seq(productModels))
      .saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/productModels")
    sc.parallelize(Seq(userStringIntMap))
      .saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/userStringIntMap")
    sc.parallelize(Seq(itemStringIntMap))
      .saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/itemStringIntMap")
    sc.parallelize(Seq(items)).saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/items")
    sc.parallelize(Seq(preparedRecs)).saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/preparedRecs")
    //userHistories.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/userHistories")
    //graph.first.vertices.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/graphvertices")
    //graph.first.edges.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/graphedges")
    true
  }

  override def toString = {
    s"userFeatures: [${userFeatures.count()}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productFeatures: [${productFeatures.count()}]" +
    s"(${productFeatures.take(2).toList}...)" +
    s" userStringIntMap: [${userStringIntMap.size}]" +
    s"(${userStringIntMap.take(2)}...)" +
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
    s"(${itemStringIntMap.take(2)}...)"
    s" items: [${items.size}]" +
    s"(${items.take(2)}...)"
  }

  def recommendProductsWithFilter(
      user: Int,
      query: Query,
      productIdBlackList: Set[Int],
      productIdWhiteList: Set[Int]) = {
    val filteredProductFeatures =  if (query.recentOnly) {
      productFeatures
      .filter(features => !productIdBlackList.contains(features._1))
      .filter(features => productIdWhiteList.contains(features._1))
    }else{
      productFeatures
      .filter(features => !productIdBlackList.contains(features._1))
    }
    recommend(userFeatures.lookup(user).head, filteredProductFeatures, query)
      .map(t => Rating(user, t._1, t._2))
  }

  private def recommend(
      recommendToFeatures: Array[Double],
      recommendableFeatures: RDD[(Int, Array[Double])],
      query: Query): Array[(Int, Double)] = {
    val recommendToVector = new DoubleMatrix(recommendToFeatures)
    val scored = recommendableFeatures.map { case (id,features) =>
      (id, recommendToVector.dot(new DoubleMatrix(features)))
    }
    if (query.serendip) {
      scored.top(30)(Ordering.by(_._2)).reverse.take(query.num)
    }else{
      scored.top(query.num)(Ordering.by(_._2))
    }
  }
/*
  def recommendProductsWithFilter(
      user: Int,
      num: Int,
      productIdFilter: Set[Int],
      userHistory:RDD[Array[Int]],fullgraph:RDD[Graph[String,Double]]
  ): Array[Rating] = {
    val filteredProductFeatures = productFeatures.filter(features => !productIdFilter.contains(features._1))
    recommend(userFeatures.lookup(user).head, filteredProductFeatures, num, userHistory, fullgraph)
      .map(t => Rating(user, t._1, t._2))
  }

  private def recommend(
      recommendToFeatures: Array[Double],
      recommendableFeatures: RDD[(Int, Array[Double])],
      num: Int,
      userHistory: RDD[Array[Int]],fullgraph:RDD[Graph[String,Double]]
    ): Array[(Int, Double,Int)] = {
    println(s"++++++++++++++++++++++++++++++++graph edge count from recommend ${fullgraph.first.edges.count}")
    println(s"++++++++++++++++++++++++++++++++userHistory from recommend ${userHistory.count}")
    val recommendToVector = new DoubleMatrix(recommendToFeatures)
    val scored:RDD[(Int,Double)] = recommendableFeatures.map { case (id,features) =>
      (id, recommendToVector.dot(new DoubleMatrix(features)))
    }

    val scoredHistory:RDD[((Int,Double),(Array[Int],Graph[String,Double]))] = scored.cartesian(userHistory.cartesian(fullgraph))
    val scoredWithTriangleCount:RDD[(Int,Double,Int)] = scoredHistory.map{ case ((id:Int, score:Double),(userHistory:Array[Int],fullgraph:Graph[String,Double])) =>
      println(s"++++++++++++++++++++++++++++++++userHistory from recommend map function ${userHistory.length}")
      println(s"++++++++++++++++++++++++++++++++graph edge count from recommend ${fullgraph.edges.count}")
      val subgraphNodes: Array[Int] = userHistory :+ id
      val subgraph = fullgraph.subgraph(vpred = (vid,attr)=> subgraphNodes contains vid.toInt)
      (id,score,findTriangles(id,subgraph))
    }
    scoredWithTriangleCount.top(num)(Ordering.by(_._2)).sortWith(_._3 < _._3).take(4)
  }
  
  def findTriangles(id:Int,subgraph:Graph[String,Double]): Int = {
        logger.info(s"findTriangles full print test")
        logger.info(s"findTriangles full graph edge count ${subgraph.edges.count}")
        println(s"++++++++++++++++++++++++++++++++test est test from findTriangles")
        println(s"++++++++++++++++++++++++++++++++graph edge count from findTriangles ${subgraph.edges.count}")
        val triCounts = subgraph.triangleCount().vertices
        val count:Int = triCounts.filter{case(item,count)=> {item.toInt == id}}.map{case(item,count)=>count}.first
        count
  }*/

}

object ALSModel
  extends IPersistentModelLoader[ALSAlgorithmParams, ALSModel] {
  def apply(id: String, params: ALSAlgorithmParams,
    sc: Option[SparkContext]) = {
    new ALSModel(
      rank = sc.get.objectFile[Int](s"/home/ubuntu/saved_models/${id}/rank").first,
      userFeatures = sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/userFeatures"),
      productFeatures = sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/productFeatures"),
      productModels = sc.get
        .objectFile[Map[Int, ProductModel]](s"/home/ubuntu/saved_models/${id}/productModels").first,
      userStringIntMap = sc.get
        .objectFile[BiMap[String, Int]](s"/home/ubuntu/saved_models/${id}/userStringIntMap").first,
      itemStringIntMap = sc.get
        .objectFile[BiMap[String, Int]](s"/home/ubuntu/saved_models/${id}/itemStringIntMap").first,
      items = sc.get
        .objectFile[Map[Int, Item]](s"/home/ubuntu/saved_models/${id}/items").first,
      preparedRecs = sc.get
        .objectFile[Map[String, Array[ItemScore]]](s"/home/ubuntu/saved_models/${id}/preparedRecs").first
      //userHistories = sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/userHistories")
      //graph = sc.get.parallelize(Array(Graph(
       // sc.get.objectFile[(VertexId,String)](s"/home/ubuntu/saved_models/${id}/graphvertices"),
       // sc.get.objectFile[Edge[Double]](s"/home/ubuntu/saved_models/${id}/graphedges")))))
    ) 
  }
}
