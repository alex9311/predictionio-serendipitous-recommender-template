package org.template.serendipitous

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap
import io.prediction.data.storage.Event
import io.prediction.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.mllib.recommendation.ALSModel

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration


case class ProductModel(
  item: Item,
  features: Option[Array[Double]]
)

case class ALSAlgorithmParams(
  appName: String,
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
      s"viewEvents in PreparedData cannot be empty." + " Please check if DataSource generates TrainingData" + " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." + " Please check if DataSource generates TrainingData" + " and Preprator generates PreparedData correctly.")
    val userStringIntMap: BiMap[String,Int] = BiMap.stringInt(data.viewEvents.map(_.user))
    val itemStringIntMap: BiMap[String,Int] = BiMap.stringInt(data.items.keys)

    val mllibRatings = data.viewEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}" + " to Int index.")
        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}" + " to Int index.")

        ((uindex, iindex), 1)
      }.filter { case ((u, i), v) => (u != -1) && (i != -1)}
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) => MLlibRating(u, i, v)}
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

    val itemsAsInt: RDD[(Int,Item)] = data.items.map { case (id, item) => (itemStringIntMap(id), item) }

    val productModels: Map[Int, ProductModel] =
      itemsAsInt.leftOuterJoin(m.productFeatures).collectAsMap.toMap
      .map { case (index, (item, features)) =>
        val pm = ProductModel(
          item = item,
          features = features
        )
        (index, pm)
      }

    val precalcModel = new ALSModel(
      rank = m.rank,
      productFeatures = m.productFeatures,
      userFeatures = m.userFeatures,
      productModels = productModels,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      itemsAsIntMap = itemsAsInt.collectAsMap.toMap,
      twoWeeksAgo = getTwoWeeksAgo(),
      preparedRecs = None
    )

    val preparedRecs: Map[String,Array[ItemScore]] = precalcRecs(sc,precalcModel,data)

    new ALSModel(
      rank = m.rank,
      productFeatures = m.productFeatures,
      userFeatures = m.userFeatures,
      productModels = productModels,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      itemsAsIntMap = itemsAsInt.collectAsMap.toMap,
      twoWeeksAgo = getTwoWeeksAgo(),
      preparedRecs = Some(preparedRecs)
    )
  }
  def precalcRecs(sc: SparkContext, model: ALSModel, data: PreparedData): Map[String,Array[ItemScore]] = {
    val users: Array[String] = data.viewEvents.map(viewEvent => (viewEvent.user)).collect.distinct

    val neighborMap: Map[VertexId,Array[VertexId]] = { data.graph.edges
      .flatMap{ edge => List((edge.srcId,edge.dstId),(edge.dstId,edge.srcId)) }
      .groupByKey
      .map{case(edge,neighbors)=>(edge,neighbors.toArray)}
      .collect.toMap
    }

    val userBaselineRecs: RDD[(String,Array[ItemScore])] = sc.parallelize({ users.map( userid => {
      val query: Query = Query(user = userid, num = 30, recommender = "baseline", recentDate=None, blackList=None)
      (userid,predict(model,query).itemScores)
    })})

    val userHistoriesBrdcst = sc.broadcast(data.userHistories)
    val neighborMapBrdcst = sc.broadcast(neighborMap)

    val triangleCounts: RDD[(String,Array[(ItemScore,Int)])] = userBaselineRecs.map { case(userid:String,baselineRecs:Array[ItemScore]) => {
      val userHistory: Array[Long] = userHistoriesBrdcst.value.getOrElse(userid,Array.empty[Long])
      val triCounts: Array[(ItemScore,Int)] = {
        baselineRecs.map(baselineRec => {
          val baselineRecId: Long = baselineRec.item.toLong
          val subgraphNodes:Array[Long] = userHistory :+ baselineRecId
          val subgraphNeighborMap: Map[VertexId,Array[VertexId]]= neighborMapBrdcst.value.filterKeys(subgraphNodes contains _.toLong)
          val recNeighbors: Array[VertexId] = subgraphNeighborMap.getOrElse(baselineRecId,Array.empty[VertexId]).filter(subgraphNodes contains _.toLong)
          val possibleTriEdges: Array[(VertexId,VertexId)] = recNeighbors.flatMap(_a => recNeighbors.map(_b => _a -> _b))
          val count: Int = possibleTriEdges.filter{case(srcId,dstId) => subgraphNeighborMap.getOrElse(srcId,Array.empty[VertexId]) contains dstId }.length
          (baselineRec,count/2)
        })
      }
      (userid,triCounts)
    }}

    val preparedRecs: Map[String,Array[ItemScore]] = {
      triangleCounts.map{ case(userid:String,potentialWithTriangle:Array[(ItemScore,Int)]) =>
        (userid,potentialWithTriangle.sortWith(_._2 < _._2).take(4))
      }
      .map{case(userid:String,potentialWithTriangle:Array[(ItemScore,Int)]) => (userid,potentialWithTriangle.map(x=>x._1))}
      .collect.toMap
    }
    preparedRecs
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {
    if(query.recommender == "serendip"){
      if(model.preparedRecs.isEmpty){
        logger.info(s"No prepared recs, running cheap serendipity instead")
        predict(model,Query(user = query.user, num = query.num, recommender = "cheap_serendip", recentDate=query.recentDate, blackList=query.blackList))
      }else{
        val itemScores: Array[ItemScore] = model.preparedRecs.get.getOrElse(query.user,{
           logger.info(s"No prediction for unknown user ${query.user}.")
           Array.empty
        })
        logger.info(s"Made (${query.recommender}) prediction for ${query.user}: ${itemScores mkString}.")
        new PredictedResult(itemScores)
      }

    }else{
      val itemIntStringMap = model.itemStringIntMap.inverse
      val convertedBlackList: Set[Int] = query.blackList.getOrElse(Set[String]()).flatMap(x=>model.itemStringIntMap.get(x))
      val recentItemList: Set[Int] = genRecentItemList(query, model, itemIntStringMap)
      model.userStringIntMap.get(query.user).map { userInt =>
        val itemScores = model
          .recommendProductsWithFilter(userInt, query, convertedBlackList, recentItemList)
            .map { r  =>
              val it = model.itemsAsIntMap(r.product)
              new ItemScore(
              title = it.title,
              category = it.category,
              date_created = it.date_created,
              item = itemIntStringMap(r.product),
              score = r.rating
            )}
        if(!model.preparedRecs.isEmpty) {logger.info(s"Made (${query.recommender}) prediction for ${query.user}: ${itemScores mkString}.")}
        new PredictedResult(itemScores)
      }.getOrElse{
        logger.info(s"No prediction for unknown user ${query.user}.")
        new PredictedResult(Array.empty)
      }
    }
  }

  def genRecentItemList(query: Query,model: ALSModel, itemIntStringMap:BiMap[Int, String]): Set[Int] = {
    val recentDate: String = query.recentDate.getOrElse(model.twoWeeksAgo)
    val productModels: Map[Int,ProductModel] = model.productModels
    productModels.filter{ case(id,pm)=>pm.item.date_created > recentDate}
      .keySet
  }

  def getTwoWeeksAgo(): String = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE,-14)
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(cal.getTime())
  }
}
