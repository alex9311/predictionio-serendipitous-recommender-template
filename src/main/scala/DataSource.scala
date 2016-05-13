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
import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import java.io._


import grizzled.slf4j.Logger

case class DataSourceParams(appName: String, graphEdgelistPath: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  def getItems(sc: SparkContext): RDD[(String, Item)] = {
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        Item(
          category= properties.get[String]("category"),
          title= properties.get[String]("title"),
          date_created= properties.get[String]("date_created"),
          categories = properties.getOpt[List[String]]("categories")) 
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()
    itemsRDD
  }
  def getViewEvents(sc:SparkContext): RDD[ViewEvent] = {
    val viewEventsRDD: RDD[ViewEvent] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))(sc)
      // eventsDb.find() returns RDD[Event]
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
      viewEventsRDD
  }

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // get items and view events into RDDs
    val itemsRDD: RDD[(String, Item)] = getItems(sc)
    println("==========================================itemsRDD size:"+itemsRDD.count)
    val viewEventsRDD: RDD[ViewEvent] = getViewEvents(sc)
    println("==========================================viewEventsRDD size:"+viewEventsRDD.count)

    //corpus used as source for training docs and input docs
    val corpus: RDD[(Long,Array[String])] = {
       viewEventsRDD.map(event => (event.item,event.user))
        .groupByKey() // you will get (itemid , Iterable[userId] )
        .filter{ case (itemid,viewers) => viewers.toArray.distinct.size > 5 }//remove items with less that 5 distinct viewers
        .map{ case(itemid,viewers) => (itemid.toLong, viewers.mkString(" ").split("\\s"))}
    }
    println("==========================================corpus size: "+corpus.count)

    //get corpus info needed for LDA stuff
    val termCounts: Array[(String, Long)] =
        corpus.flatMap(_._2.map(_ -> 1L)).reduceByKey(_ + _).filter(x=> x._2>1).collect().sortBy(-_._2)
    println("==========================================termcounts size: "+termCounts.size)
    val vocabArray: Array[String] =
        termCounts.takeRight(termCounts.size).map(_._1)
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    //get graphdocs with their user count vectors
    val graphdocs: RDD[(Long, Vector)] =
        corpus
       //   .filter{case(id:Long,views:Array[String])=>{ ( id.toInt%2 == 0 )}}//filter to make input smaller
          .map { case (id:Long, tokens:Array[String]) =>
            (id, Vectors.sparse(vocab.size, LdaSimHelpers.get_termcount(tokens,vocab).toSeq))
        }
    println("==========================================graphdocs size: "+graphdocs.count)
	
    //Item info for items to be included in graph
    val graphItems:RDD[(String,Item)] = graphdocs.map{
      case(itemid:Long,viewers:Vector)=> (itemid.toString,1)
    }.join(itemsRDD).map{ case(itemid:String,(t:Int,item:Item))=> (itemid,item) }
    println("==========================================graphItems size:"+graphItems.count)

    //get user counts for all docs in corpus for training set
    val trainingdocs: RDD[(Long, Vector)] =
        corpus.map { case (id:Long, tokens:Array[String]) =>
            (id, Vectors.sparse(vocab.size, LdaSimHelpers.get_termcount(tokens,vocab).toSeq))
        }

    // Set LDA parameters
    val ldaModel = new LDA().setK(80).setMaxIterations(60).run(trainingdocs)

/*
    for(numTopics<-Seq(60,65,70,75,80);iterations<-Seq(60)) {
      val ldaModel = new LDA().setK(numTopics).setMaxIterations(iterations).run(trainingdocs)
      val avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood / trainingdocs.count()
      println("============(topics: "+numTopics+", iterations: "+iterations+") avgLogLikelihood: "+avgLogLikelihood)
    }*/

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
/*
    topicIndices.foreach { case (terms, termWeights) =>
        println("TOPIC:")
        terms.zip(termWeights).foreach { case (term, weight) =>
             println(s"${vocabArray(term.toInt)}\t$weight")
        }
        println()
    }*/


    ldaModel.save(sc, "myLDAModel")
    val localLdaModel = DistributedLDAModel.load(sc, "myLDAModel").toLocal


    val topicDistributions:collection.Map[Long,Vector] = localLdaModel.topicDistributions(graphdocs).collectAsMap()
    //for((k,v) <- topicDistributions) printf("itemid: %s, topic vector (%s)\n",k,v.toArray.mkString(", "))
    println("finished calculating topic distributions")
    
    //graphItems.map{case(itemid:String,item:Item)=> (itemid,topicDistributions(itemid.toLong))}.saveAsTextFile("output")

    val edges: RDD[(Int,Int,Double)] = LdaSimHelpers.calculateEdges(graphItems.map{case(itemid:String,item:Item)=>(itemid.toInt,topicDistributions(itemid.toLong).toArray)},sc)
    println("number of edges: "+edges.count)
    println("finished calculating edges, going to print")

    LdaSimHelpers.gephiPrint(edges,graphItems)

    /*
    val edges: RDD[Edge[String]] =
      itemsRDD.map{
        x => Edge(x._1, 1, 1)
      }
    */
    //val g : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
    val g = GraphLoader.edgeListFile(sc, dsp.graphEdgelistPath)

    new TrainingData(
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      graph = g
    )
  }
}

case class User()

//case class Item(categories: Option[List[String]])
case class Item(
    title: String,
    category: String,
    date_created: String,
    categories: Option[List[String]])

case class ViewEvent(user: String, item: String, t: Long)

class TrainingData(
  //val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
  val graph: Graph[Int,Int]
) extends Serializable {
  override def toString = {
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)"
  }
}
