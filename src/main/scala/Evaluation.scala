package org.template.serendipitous

import io.prediction.controller.Evaluation
import io.prediction.controller.OptionAverageMetric
import io.prediction.controller.AverageMetric
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EngineParamsGenerator
import io.prediction.controller.EngineParams
import io.prediction.controller.MetricEvaluator

import grizzled.slf4j.Logger

case class PrecisionAtK(k: Int) extends OptionAverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  require(k > 0, "k must be greater than 0")
  override def header = s"Precision@K (k=$k)"

  def calculate(q: Query, p: PredictedResult, a: ActualResult): Option[Double] = {
    val positives: Set[String] = a.viewEvents.map(_.item).toSet

    if (positives.size == 0) { return None }

    val tpCount: Int = p.itemScores.take(k).filter(is => positives(is.item)).size

    Some(tpCount.toDouble / math.min(k, positives.size))
  }
}

case class PositiveCount()
    extends AverageMetric[EmptyEvaluationInfo, Query, PredictedResult, ActualResult] {
  override def header = s"PositiveCount"

  def calculate(q: Query, p: PredictedResult, a: ActualResult): Double = {
    a.viewEvents.size
  }
}

object RecommendationEvaluation extends Evaluation {
  engineEvaluator = (
    SerendipitousEngine(),
    MetricEvaluator(
      metric = PrecisionAtK(k = 4),
      otherMetrics = Seq(
        PositiveCount()
      )))
}

trait BaseEngineParamsList extends EngineParamsGenerator {
  protected val baseEP = EngineParams(
    dataSourceParams = DataSourceParams(
      appName = "Serendip",
      evalParams = Some(DataSourceEvalParams(kFold = 5, queryNum = 4))))
}

object EngineParamsList extends BaseEngineParamsList {
  engineParamsList = for(
    rank <- Seq(10);
    numIterations <- Seq(1, 5))
    yield baseEP.copy(
      algorithmParamsList = Seq(
        ("als", ALSAlgorithmParams("Serendip",rank, numIterations, 0.01, Some(3)))))
}

