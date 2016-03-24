package org.template.similarproduct

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

import org.apache.spark.mllib.recommendation.ALSModel

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue

case class ALSAlgorithmParams(
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]) extends Params

/**
  * Use ALS to build item x feature matrix
  */
class ALSAlgorithm(val ap: ALSAlgorithmParams)
  extends P2LAlgorithm[PreparedData, ALSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): ALSModel = {
    require(!data.viewEvents.take(1).isEmpty,
      s"viewEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    // create User and item's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.viewEvents.map(_.user))
    //val userStringIntMap = BiMap.stringInt(data.users.keys)
    val itemStringIntMap = BiMap.stringInt(data.items.keys)

    // collect Item as Map and convert ID to Int index
    val items: Map[Int, Item] = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }.collectAsMap.toMap

    val mllibRatings = data.viewEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), 1)
      }.filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }.reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) =>
        // MLlibRating requires integer index for user and item
        MLlibRating(u, i, v)
      }
      .cache()

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid user and item ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)

    new ALSModel(
      rank = m.rank,
      productFeatures = m.productFeatures,
      userFeatures = m.userFeatures,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      items = items
    )
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {
  // Convert String ID to Int index for Mllib
    model.userStringIntMap.get(query.user).map { userInt =>
      // create inverse view of itemStringIntMap
      val itemIntStringMap = model.itemStringIntMap.inverse
      // recommendProducts() returns Array[MLlibRating], which uses item Int
      // index. Convert it to String ID for returning PredictedResult
      val itemScores = model.recommendProducts(userInt, query.num)
          .map { r  =>
            val it = model.items(r.product)
            new ItemScore(
            title = it.title,
            category = it.category,
            item = itemIntStringMap(r.product),
            score = r.rating
          )}
      new PredictedResult(itemScores)
    }.getOrElse{
      logger.info(s"No prediction for unknown user ${query.user}.")
      new PredictedResult(Array.empty)
    }
  }
/*
  private
  def getTopN[T](s: Seq[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateItem(
    i: Int,
    items: Map[Int, Item],
    categories: Option[Set[String]],
    queryList: Set[Int],
    whiteList: Option[Set[Int]],
    blackList: Option[Set[Int]]
  ): Boolean = {
    whiteList.map(_.contains(i)).getOrElse(true) &&
    blackList.map(!_.contains(i)).getOrElse(true) &&
    // discard items in query as well
    (!queryList.contains(i)) &&
    // filter categories
    categories.map { cat =>
      items(i).categories.map { itemCat =>
        // keep this item if has ovelap categories with the query
        !(itemCat.toSet.intersect(cat).isEmpty)
      }.getOrElse(false) // discard this item if it has no categories
    }.getOrElse(true)
  }*/

}
