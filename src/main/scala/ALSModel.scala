package org.apache.spark.mllib.recommendation
// This must be the same package as Spark's MatrixFactorizationModel because
// MatrixFactorizationModel's constructor is private and we are using
// its constructor in order to save and load the model

import org.template.serendipitous.ALSAlgorithmParams
import org.template.serendipitous.evaldev._

import io.prediction.controller.IPersistentModel
import io.prediction.controller.IPersistentModelLoader
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


import org.jblas.DoubleMatrix


class ALSModel(
    override val rank: Int,
    override val userFeatures: RDD[(Int, Array[Double])],
    override val productFeatures: RDD[(Int, Array[Double])],
    val userStringIntMap: BiMap[String, Int],
    val itemStringIntMap: BiMap[String, Int],
    val items: Map[Int, Item],
    val graph: Graph[Int,Int])
  extends MatrixFactorizationModel(rank, userFeatures, productFeatures)
  with IPersistentModel[ALSAlgorithmParams] {

  def save(id: String, params: ALSAlgorithmParams,
    sc: SparkContext): Boolean = {

    sc.parallelize(Seq(rank)).saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/rank")
    userFeatures.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/userFeatures")
    productFeatures.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/productFeatures")
    sc.parallelize(Seq(userStringIntMap))
      .saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/userStringIntMap")
    sc.parallelize(Seq(itemStringIntMap))
      .saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/itemStringIntMap")
    sc.parallelize(Seq(items)).saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/items")
    graph.vertices.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/graphvertices")
    graph.edges.saveAsObjectFile(s"/home/ubuntu/saved_models/${id}/graphedges")
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
      num: Int,
      productIdFilter: Set[Int]) = {
    val filteredProductFeatures = productFeatures
      .filter(features => !productIdFilter.contains(features._1)) // (*)
    recommend(userFeatures.lookup(user).head, filteredProductFeatures, num)
      .map(t => Rating(user, t._1, t._2))
  }

  private def recommend(
      recommendToFeatures: Array[Double],
      recommendableFeatures: RDD[(Int, Array[Double])],
      num: Int): Array[(Int, Double)] = {
    val recommendToVector = new DoubleMatrix(recommendToFeatures)
    val scored = recommendableFeatures.map { case (id,features) =>
      (id, recommendToVector.dot(new DoubleMatrix(features)))
    }
    scored.top(num)(Ordering.by(_._2))
  }

}

object ALSModel
  extends IPersistentModelLoader[ALSAlgorithmParams, ALSModel] {
  def apply(id: String, params: ALSAlgorithmParams,
    sc: Option[SparkContext]) = {
    new ALSModel(
      rank = sc.get.objectFile[Int](s"/home/ubuntu/saved_models/${id}/rank").first,
      userFeatures = sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/userFeatures"),
      productFeatures = sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/productFeatures"),
      userStringIntMap = sc.get
        .objectFile[BiMap[String, Int]](s"/home/ubuntu/saved_models/${id}/userStringIntMap").first,
      itemStringIntMap = sc.get
        .objectFile[BiMap[String, Int]](s"/home/ubuntu/saved_models/${id}/itemStringIntMap").first,
      items = sc.get
        .objectFile[Map[Int, Item]](s"/home/ubuntu/saved_models/${id}/items").first,
      graph = Graph(sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/graphvertices"),sc.get.objectFile(s"/home/ubuntu/saved_models/${id}/graphedges")))
  }
}
