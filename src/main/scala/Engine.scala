package org.template.serendipitous

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  user: String,
  num: Int,
  unseenOnly:Boolean,
  recentOnly:Boolean,
  serendip:Boolean,
  blackList: Option[Set[String]],
  recentDate: Option[String]
) extends Serializable

case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable {
  override def toString: String = itemScores.mkString(",")
}

case class ItemScore(
  item: String,
  score: Double,
  title: String,
  date_created: String,
  category: String
) extends Serializable

object SerendipitousEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map(
        "als" -> classOf[ALSAlgorithm]),
      classOf[Serving])
  }
}
