package org.template.serendipitous

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      items = trainingData.items,
      viewEvents = trainingData.viewEvents,
      userHistories = trainingData.userHistories,
      graph = trainingData.graph)
  }
}

class PreparedData(
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
  val userHistories: Map[String,Array[Long]],
  val graph: Graph[String,Double]
) extends Serializable
