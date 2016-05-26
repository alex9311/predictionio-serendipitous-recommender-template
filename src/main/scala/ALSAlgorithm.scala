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

import org.apache.spark.mllib.recommendation.ALSModel

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration

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
    logger.info(s"graph!!!! in model training ${data.graph.numEdges.toString}")
    logger.info(s"edges: ${data.graph.numEdges.toString}")
    logger.info(s"nodes: ${data.graph.numVertices.toString}")
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

    val userHistoriesMap = data.userHistories.collectAsMap.toMap
    logger.info(s"!!!!!!! userHistoriesMap count: ${userHistoriesMap.keySet.size}")
      
    new ALSModel(
      rank = m.rank,
      productFeatures = m.productFeatures,
      userFeatures = m.userFeatures,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      items = items,
      userHistories = userHistoriesMap,
      graph = data.graph
    )
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {
    val userHistory:Array[Int] = model.userHistories(query.user)
    //logger.info(s"user history for user ${query.user}: ${userHistory.mkString(", ")}.")
    //logger.info(s"full graph edge count ${model.graph.edges.count}")
    
    //val subgraph = model.graph.subgraph
    val convertedBlackList: Set[Int] = genBlackList(query = query)
      .flatMap(x=>model.itemStringIntMap.get(x))
  // Convert String ID to Int index for Mllib
    model.userStringIntMap.get(query.user).map { userInt =>
      // create inverse view of itemStringIntMap
      val itemIntStringMap = model.itemStringIntMap.inverse
      // recommendProducts() returns Array[MLlibRating], which uses item Int
      // index. Convert it to String ID for returning PredictedResult
      val itemScores:Array[ItemScore] = model
        .recommendProductsWithFilter(userInt, 10,convertedBlackList)
          .map { r  =>
            val it = model.items(r.product)
            new ItemScore(
            title = it.title,
            category = it.category,
            item = itemIntStringMap(r.product),
            score = r.rating
          )}

      logger.info(s"Made prediction for user ${query.user}: ${itemScores mkString}.")
      logger.info(s"userHistory size: (${query.user}):${userHistory.size}")
      logger.info(s"userHistory: (${query.user}): (${userHistory.mkString("), (")})")

      val predictedTriangles: Array[(ItemScore,Int)] = itemScores.map{score => {
        val predictedItem = score.item.toInt
        val graphItems: Array[Int] = userHistory :+ predictedItem  
        val subgraph = model.graph.subgraph(vpred = (id,attr) => graphItems contains id.toInt)
	val triCounts = subgraph.triangleCount().vertices
	logger.info(s"subgraph edges (${predictedItem.toString}):${subgraph.edges.count}")
        subgraph.edges.foreach( edge => logger.info(s"${edge.toString}"))

	logger.info(s"subgraph vertices (${predictedItem.toString}):${subgraph.vertices.count}")
        subgraph.vertices.foreach( vert => logger.info(s"${vert.toString}"))

	logger.info(s"triCount vertices ${triCounts.count}")
        val count = triCounts.filter{case(item,count)=> {item.toInt == predictedItem}}.map{case(item,count)=>count}.first
        logger.info(s"(${predictedItem},${count})")
        (score,count)
      }}.toArray

      logger.info(s"predictedTriangles:")
      

      val boostedScores:Array[(ItemScore,Int)] = predictedTriangles.sortWith(_._2 < _._2)
      boostedScores.foreach( tri => logger.info(s"${tri.toString}"))

      //val edgeStrings:RDD[String] = subgraph.edges.map(edge=>edge.srcId.toString+","+edge.dstId.toString+","+edge.attr+",Undirected")
      //edgeStrings.saveAsTextFile("edges_subgraph")

      new PredictedResult(boostedScores.take(query.num).map{case(item,triangles)=> {item}})

    }.getOrElse{
      logger.info(s"No prediction for unknown user ${query.user}.")
      new PredictedResult(Array.empty)
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
