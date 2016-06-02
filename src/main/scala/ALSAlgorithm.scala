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
      s"viewEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    // create User and item's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.viewEvents.map(_.user))
    val itemStringIntMap = BiMap.stringInt(data.items.keys)

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

    val items = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }

    val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
      items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    val productModels: Map[Int, ProductModel] = productFeatures
      .map { case (index, (item, features)) =>
        val pm = ProductModel(
          item = item,
          features = features
        )
        (index, pm)
      }

    val stubPreparedRecs: scala.collection.immutable.Map[String,Array[ItemScore]] = 
      data.viewEvents.map(viewEvent =>(viewEvent.user,viewEvent.item.toInt))
        .groupByKey()
        .map{case(userid:String,items:Iterable[Int]) => { 
          (userid,Array(ItemScore("item",1.0,"title","date","category")))
        }}
      .collect()
      .toMap
    
    logger.info(s"creating stub model")

    val model = new ALSModel(
      rank = m.rank,
      productFeatures = m.productFeatures,
      userFeatures = m.userFeatures,
      productModels = productModels,
      preparedRecs = stubPreparedRecs,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      items = items.collectAsMap.toMap
    )
    
    logger.info(s"finished creating stub model")
    logger.info(s"creating prepared recs")

    val users: Array[String] = data.viewEvents.map(viewEvent => (viewEvent.user)).collect.distinct

    logger.info(s"calculating neighbor map")
    val neighborMap: Map[VertexId,Array[VertexId]] = { data.graph.edges
      .flatMap{ edge => List((edge.srcId,edge.dstId),(edge.dstId,edge.srcId)) }
      .groupByKey
      .map{case(edge,neighbors)=>(edge,neighbors.toArray)}
      .collect.toMap
    }

    logger.info(s"calculating potentialRecs")
    val userBaselineRecs: RDD[(String,Array[ItemScore])] = sc.parallelize({ users.map( userid => {
      val query: Query = Query(user = userid, num = 20, unseenOnly = false, recentOnly = true, serendip = false, recentDate=None, blackList=None)
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

    logger.info(s"finished creating prpared recs")

    new ALSModel(
      rank = m.rank,
      productFeatures = m.productFeatures,
      userFeatures = m.userFeatures,
      productModels = productModels,
      preparedRecs = preparedRecs,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      items = items.collectAsMap.toMap
    )
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {
    val productModels = model.productModels
    val itemIntStringMap = model.itemStringIntMap.inverse
    val convertedBlackList: Set[Int] = genBlackList(query = query)
      .flatMap(x=>model.itemStringIntMap.get(x))
    val convertedWhiteList: Set[Int] = genWhiteList(query = query,productModels=productModels,itemIntStringMap)
      .flatMap(x=>model.itemStringIntMap.get(x))
    // Convert String ID to Int index for Mllib
    logger.info(s"in predict for ${query.user}, serendip is ${query.serendip.toString}")
    if(query.serendip == false){
      logger.info(s"creating baseline predict for ${query.user}") 
      model.userStringIntMap.get(query.user).map { userInt =>
        // create inverse view of itemStringIntMap
        // recommendProducts() returns Array[MLlibRating], which uses item Int
        // index. Convert it to String ID for returning PredictedResult
        val itemScores = model
          .recommendProductsWithFilter(userInt, query, convertedBlackList, convertedWhiteList)
            .map { r  =>
              val it = model.items(r.product)
              new ItemScore(
              title = it.title,
              category = it.category,
              date_created = it.date_created,
              item = itemIntStringMap(r.product),
              score = r.rating
            )}
        logger.info(s"Made prediction for user ${query.user}: ${itemScores mkString}.")
        new PredictedResult(itemScores)
      }.getOrElse{
        logger.info(s"No prediction for unknown user ${query.user}.")
        new PredictedResult(Array.empty)
      }
    }else{
      logger.info(s"creating serendip predict for ${query.user}") 
      new PredictedResult(model.preparedRecs(query.user))
    }
  }

  def genWhiteList(query: Query,productModels:Map[Int,ProductModel],itemIntStringMap:BiMap[Int, String]): Set[String] = {
    val recentDate = query.recentDate.getOrElse("2016-01-01")
    // if recentOnly is True, get all recent items
    if (query.recentOnly) {
	productModels.filter{ case(id,pm)=>pm.item.date_created > recentDate}
        .keySet
	.flatMap(x=>itemIntStringMap.get(x))
	.toSet
    } else {
      Set[String]()
    }
  }

  def genBlackList(query: Query): Set[String] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[String] = if (query.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user,
          eventNames = Some(List("view")),
          targetEntityType = Some(Some("item")),
          // set time limit to avoid super long DB access
          timeout = Duration(2000, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }
    query.blackList.getOrElse(Set[String]()) ++ seenItems
  }
}
