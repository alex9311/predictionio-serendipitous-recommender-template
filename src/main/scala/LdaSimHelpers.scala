package org.template.serendipitous

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object LdaSimHelpers {
  def gephiPrint(edges:RDD[(String,String,Double)],items:RDD[(String,Item)]) = {
    val gephiEdges:RDD[String] = edges.map{case(item1,item2,weight)=> item1+","+item2+","+weight.toString+",Undirected\n"}
    gephiEdges.saveAsTextFile("gephi/edges")

    val gephiVertices:RDD[String] = items.map{case(itemid,item)=> itemid+","+item.category+"\n"}
    gephiVertices.saveAsTextFile("gephi/vertices")
  }

  def combs(rdd:RDD[String],sc:SparkContext):RDD[(String,String)] = {
    val count = rdd.count
    if (rdd.count < 2) { 
        sc.makeRDD[(String,String)](Seq.empty)
    } else if (rdd.count == 2) {
        val values = rdd.collect
        sc.makeRDD[(String,String)](Seq((values(0), values(1))))
    } else {
        val elem = rdd.take(1)
        val elemRdd = sc.makeRDD(elem)
        val subtracted = rdd.subtract(elemRdd)  
        val comb = subtracted.map(e  => (elem(0),e))
        comb.union(combs(subtracted,sc))
    } 
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
  def cosineSimilarity(x: Array[Double], y: Array[Double]): Double = {
    require(x.size == y.size)
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  
  def magnitude(x: Array[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }
}
