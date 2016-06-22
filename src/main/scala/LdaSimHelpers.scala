package org.template.serendipitous

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import scala.reflect.ClassTag
import scala.collection.mutable
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, LocalLDAModel, LDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


object LdaSimHelpers {
  def gephiPrint(edges:RDD[(Int,Int,Double)],items:RDD[(String,Item)]) = {
    val outDir: String = "gephi/"
    println("saving graph to "+outDir+" in gephi import-ready format")
    val gephiVertices:RDD[String] = items.map{case(itemid,item)=> itemid+","+item.category+"\n"}
    gephiVertices.saveAsTextFile(outDir+"vertices")
    val gephiEdges:RDD[String] = edges.map{case(item1,item2,weight)=> item1.toString+","+item2.toString+","+weight.toString+",Undirected\n"}
    gephiEdges.saveAsTextFile(outDir+"edges")
  }

  def calculateEdges(nodes:RDD[(Int,Array[Double])],sc:SparkContext):RDD[(Int,Int,Double)] = {
    val nodeList = nodes.collect().toList
	/*
    val allDistances: RDD[(Int,Int,Double)] = nodes.flatMap(item1 => nodeList
      .filter(item2 => item2._1 > item1._1)
      .map(item2 => 
        (item1._1, item2._1,cosineDistance(item1._2,item2._2)))
    )*/

/*
    println(s"+++++++++total distances found is: ${allDistances.count.toString}")
    val minDistance:Double = allDistances
      .map{case(item1:Int,item2:Int,distance:Double)=>(distance,1)}
      .sortByKey(true)
      .keys.take(1).head
    println(s"+++++++++minDistance is: ${minDistance.toString}")
    val maxDistance:Double = allDistances
      .map{case(item1:Int,item2:Int,distance:Double)=>(distance,1)}
      .sortByKey(false)
      .keys.take(1).head
    println(s"+++++++++maxDistance is: ${maxDistance.toString}")
*/
   //val distanceCutoff: Double = 0.5
/*
    val distanceCutoff:Double = allDistances
      .map{case(item1:Int,item2:Int,distance:Double)=>(distance,1)}
      .sortByKey(true)
      .keys.take(nodeList.length*150)
      .reverse
      .take(1).head
    println(s"+++++++++distance cutoff is ${distanceCutoff.toString}")
*/
    //allDistances.filter{case(item1,item2,distance)=> distance < distanceCutoff}

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

  def evaluateLda(trainingdocs:RDD[(Long,Vector)]) = {
    for(numTopics<-Seq(10,20,30,40,50,60,70,80,90,100,110);iterations<-Seq(60)) {
      //val ldaModel = new LDA().setK(numTopics).setMaxIterations(iterations).run(trainingdocs)
      //val avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood / trainingdocs.count()
      val ldaParams: LDA = new LDA().setK(numTopics).setMaxIterations(iterations)
      val distributedLDAModel: DistributedLDAModel = ldaParams.run(trainingdocs).asInstanceOf[DistributedLDAModel]
      val avgLogLikelihood = distributedLDAModel.logLikelihood / trainingdocs.count()
      println("============(topics: "+numTopics+", iterations: "+iterations+") avgLogLikelihood: "+avgLogLikelihood)
    }
  }

  def printLdaTopics(ldaModel: LDAModel, vocabArray:Array[String]) = {
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10) //this one too
    topicIndices.foreach { case (terms, termWeights) =>
        println("TOPIC:")
        terms.zip(termWeights).foreach { case (term, weight) =>
             println(s"${vocabArray(term.toInt)}\t$weight")
        }
        println()
    }
  }

}
