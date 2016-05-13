package org.template.serendipitous

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object LdaSimHelpers {
  def gephiPrint(edges:RDD[(Int,Int,Double)],items:RDD[(String,Item)]) = {
    println("going to output vertices")
    val gephiVertices:RDD[String] = items.map{case(itemid,item)=> itemid+","+item.category+"\n"}
    gephiVertices.saveAsTextFile("gephi/vertices")
    println("going to output edges")
    val gephiEdges:RDD[String] = edges.map{case(item1,item2,weight)=> item1.toString+","+item2.toString+","+weight.toString+",Undirected\n"}
    gephiEdges.saveAsTextFile("gephi/edges")
  }

  def calculateEdges(nodes:RDD[(Int,Array[Double])],sc:SparkContext):RDD[(Int,Int,Double)] = {
    val nodeList = nodes.collect().toList
    nodes
      .flatMap(item1 => nodeList
        .filter(item2 => item2._1 > item1._1)
        .map(item2 => 
          (item1._1, item2._1,cosineDistance(item1._2,item2._2)))
        .filter{case(item1,item2,distance)=> distance < .5}
    )
  }

  def get_termcount(tokens:Array[String],vocab:Map[String,Int]):mutable.HashMap[Int,Double] = {
    val counts = new mutable.HashMap[Int, Double]()
    tokens.foreach { term =>
      if (vocab.contains(term)) {
          val idx = vocab(term)
          counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
      }
    }
    counts
  }
  def cosineDistance(x: Array[Double], y: Array[Double]): Double = {
    require(x.size == y.size)
    1.0 - (dotProduct(x, y)/(magnitude(x) * magnitude(y)))
  }
  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  
  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }
}
