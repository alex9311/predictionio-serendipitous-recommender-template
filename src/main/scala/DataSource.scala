package org.template.serendipitous

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore
import io.prediction.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.collection.mutable
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, LocalLDAModel, LDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io._

import grizzled.slf4j.Logger


case class DataSourceEvalParams(kFold: Int, queryNum: Int)

case class DataSourceParams(appName: String, evalParams: Option[DataSourceEvalParams]) extends Params


class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    val itemsRDD: RDD[(String, Item)] = getItems(sc)
    val viewEventsRDD: RDD[ViewEvent] = getViewEvents(sc)
    val corpusInfo: CorpusInfo = getCorpusInfo(viewEventsRDD, itemsRDD)
    logger.info(s"corpusInfo: ${corpusInfo.toString}")
    val userHistories: Map[String,Array[Long]] = getUserHistories(viewEventsRDD,corpusInfo)

    val ldaParams: LDA = new LDA().setK(80).setMaxIterations(60)
    val distributedLDAModel: DistributedLDAModel = ldaParams.run(corpusInfo.docTermVectors).asInstanceOf[DistributedLDAModel]
    val localLdaModel: LocalLDAModel = distributedLDAModel.toLocal
    val topicDistributions:Map[Long,Vector] = localLdaModel.topicDistributions(corpusInfo.docTermVectors).collect.toMap

    val graphEdges: RDD[Edge[Double]] = calculateGraphEdges(sc, corpusInfo.docItemInfo, topicDistributions)
    val graph : Graph[String, Double] = Graph.fromEdges(graphEdges,"defaultProperty")
 
    new TrainingData(
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      userHistories = userHistories,
      graph = Some(graph)
    )
  }

  def getViewEvents(sc:SparkContext): RDD[ViewEvent] = {
    val viewEventsRDD: RDD[ViewEvent] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view")),
      targetEntityType = Some(Some("item")))(sc)
      .map { event =>
        val viewEvent = try {
          event.event match {
            case "view" => ViewEvent(
              user = event.entityId,
              item = event.targetEntityId.get,
              t = event.eventTime.getMillis)
            case _ => throw new Exception(s"Unexpected event ${event} is read.")
          }
        } catch {
          case e: Exception => {
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
          }
        }
        viewEvent
      }.cache()
      logger.info("viewEventsRDD size:"+viewEventsRDD.count)
      viewEventsRDD
  }

  def getItems(sc: SparkContext): RDD[(String, Item)] = {
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        Item(
          category= properties.get[String]("category"),
          title= properties.get[String]("title"),
          date_created= properties.get[String]("date_created"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()
    logger.info("==========================================itemsRDD size:"+itemsRDD.count)
    itemsRDD
  }

  def getUserHistories(viewEventsRDD: RDD[ViewEvent],corpusInfo: CorpusInfo): Map[String,Array[Long]] = {
    val userHistories: Map[String,Array[Long]] = {
      viewEventsRDD.map(viewEvent =>(viewEvent.user,viewEvent.item.toLong))
      .groupByKey()
      .map{case(userid:String,items:Iterable[Long]) => (userid,items.toArray.distinct)}
      .filter{ case(userid,items) => corpusInfo.vocab.contains(userid)}
      .collect().toMap
    }
    userHistories
  }

  def calculateGraphEdges(sc: SparkContext, docItemInfo: RDD[(String,Item)], topicDistributions: Map[Long,Vector]): RDD[Edge[Double]] = {
    val itemTopicDistributions: RDD[(Int,Array[Double])] = 
      docItemInfo.map{ case(itemid: String,item: Item) => 
        val topicDistribution:Array[Double] = topicDistributions(itemid.toLong).toArray
        (itemid.toInt,topicDistribution)
      } 

    val edges: RDD[(Int,Int,Double)] = LdaSimHelpers.calculateEdges(itemTopicDistributions,sc)
    //LdaSimHelpers.gephiPrint(edges,docItemInfo)

    edges.map { case(item1:Int,item2:Int,weight:Double) => Edge(item1.toLong,item2.toLong,weight) }
  }

  def getCorpusInfo(viewEventsRDD: RDD[ViewEvent], itemsRDD: RDD[(String,Item)]): CorpusInfo = {
    val corpus: RDD[(Long,Array[String])] = {
       viewEventsRDD.map(event => (event.item,event.user))
        .groupByKey()
        .filter{ case (itemid,viewers) => viewers.toArray.distinct.size > 2 }//remove items with less that 2 distinct viewers
        .map{ case(itemid,viewers) => (itemid.toLong, viewers.mkString(" ").split("\\s"))}
    }

    val termCounts: Array[(String, Long)] =
        corpus.flatMap(_._2.map(_ -> 1L)).reduceByKey(_ + _).filter(x=> x._2>1).collect().sortBy(-_._2)
    val vocabArray: Array[String] =
        termCounts.takeRight(termCounts.size).map(_._1)
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    val docTermVectors: RDD[(Long, Vector)] =
      corpus.map { case (id:Long, tokens:Array[String]) =>
        (id, Vectors.sparse(vocab.size, LdaSimHelpers.get_termcount(tokens,vocab).toSeq))
      }
	
    //Item info for items to be included in graph
    val docItemInfo:RDD[(String,Item)] = docTermVectors.map{
      case(itemid:Long,viewers:Vector)=> (itemid.toString,1)
    }.join(itemsRDD).map{ case(itemid:String,(t:Int,item:Item))=> (itemid,item) }
   
    new CorpusInfo(vocab = vocab, docTermVectors = docTermVectors, docItemInfo = docItemInfo)
  }

  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(!dsp.evalParams.isEmpty, "Must specify evalParams")
    logger.info("reading eval training data")
    val evalParams = dsp.evalParams.get
    val kFold = evalParams.kFold

    val itemsRDD = getItems(sc)
    val eventsRDD = getViewEvents(sc)
    val corpusInfo: CorpusInfo = getCorpusInfo(eventsRDD, itemsRDD)
    val userHistories: Map[String,Array[Long]] = getUserHistories(eventsRDD,corpusInfo)

    val userItemViews: RDD[(ViewEvent, Long)] = eventsRDD.map { r =>
        ((r.user, r.item), 1)
      }.reduceByKey((_,_)=>1) // aggregate all view events of same user-item pair
      .map { case ((user, item), v) =>
        ViewEvent(user, item, v)
      }
      .zipWithUniqueId
    userItemViews.cache

    (0 until kFold).map { idx => {
      val trainingRatings = userItemViews.filter(_._2 % kFold != idx).map(_._1)
      val testingRatings = userItemViews.filter(_._2 % kFold == idx).map(_._1)

      //create RDD of userid, and itemViews of that user
      val testingUsers: RDD[(String, Iterable[ViewEvent])] = testingRatings.groupBy(_.user)

      logger.info("creating new training data set")

      (new TrainingData(items = itemsRDD, viewEvents = eventsRDD, userHistories = userHistories, graph = None),
        new EmptyEvaluationInfo(),
        testingUsers.map {
          case (user, itemViews) => (Query(user = user, num = evalParams.queryNum, recommender = "baseline", blackList = Some(Set[String]()), recentDate = Some("1970-01-01") ), ActualResult(itemViews.toArray))
        }
      )
    }}
  }
}

case class Item(
    title: String,
    category: String,
    date_created: String)

case class CorpusInfo( 
  val vocab: Map[String, Int], 
  val docTermVectors: RDD[(Long,Vector)], 
  val docItemInfo: RDD[(String,Item)]
) extends Serializable {
  override def toString = {
    s"vocab length: [${vocab.keySet.size}\n" + 
    s"docTermVectors: [${docTermVectors.count()}] (${docTermVectors.take(2).toList}...)\n"+
    s"docItemInfo: [${docItemInfo.count()}] (${docItemInfo.take(2).toList}...)\n"
  }
}

case class ViewEvent(user: String, item: String, t: Long)

class TrainingData(
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
  val userHistories: Map[String,Array[Long]],
  val graph: Option[Graph[String,Double]]
) extends Serializable {
  override def toString = {
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }
}
