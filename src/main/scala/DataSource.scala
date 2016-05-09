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


  def combs(rdd:RDD[String],sc:SparkContext):RDD[(String,String)] = {
    val count = rdd.count
    if (rdd.count < 2) { 
        sc.makeRDD[(String,String)](Seq.empty)
    } else if (rdd.count == 2) {
        val values = rdd.collect
        sc.makeRDD[(String,String)](Seq((values(0), values(1))))
    } else {
        val elem = rdd.take(1)
        val elemRdd = sc.makeRDD(elem)
        val subtracted = rdd.subtract(elemRdd)  
        val comb = subtracted.map(e  => (elem(0),e))
        comb.union(combs(subtracted,sc))
    } 
  }

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // get items and view events into RDDs
    val itemsRDD: RDD[(String, Item)] = getItems(sc)
    println("==========================================itemsRDD size:"+itemsRDD.count)
    val viewEventsRDD: RDD[ViewEvent] = getViewEvents(sc)
    println("==========================================viewEventsRDD size:"+viewEventsRDD.count)

    val corpus: RDD[Seq[String]] = {
       viewEventsRDD.map(event => (event.item,event.user))
        .groupByKey() // you will get (itemid , Iterable[userId] )
        .map{ case(itemid,viewers) => viewers.mkString(" ").split("\\s")}
    }
    println("==========================================corpus size: "+corpus.count)
    println("==========================================corpus first 5:")
    corpus.take(5).foreach(println)

    val termCounts: Array[(String, Long)] =
        corpus.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    println("==========================================termcounts size: "+termCounts.size)
    println("==========================================termcounts:")
    termCounts.foreach{ case(term,count)=>
        println(term+": "+count)
    }

    val vocabArray: Array[String] =
        termCounts.takeRight(termCounts.size).map(_._1)
    val vocab: Map[String, Int] = vocabArray.zipWithIndex.toMap

    // Convert documents into term count vectors
    val documents: RDD[(Long, Vector)] =
        corpus.zipWithIndex.map { case (tokens, id) =>
            val counts = new mutable.HashMap[Int, Double]()
            tokens.foreach { term =>
                if (vocab.contains(term)) {
                    val idx = vocab(term)
                    counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
                }
            }
            (id, Vectors.sparse(vocab.size, counts.toSeq))
        }
    // Set LDA parameters
    val numTopics = 10
    val ldaModel = new LDA().setK(numTopics).setMaxIterations(20).run(documents)

    //val avgLogLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood / documents.count()

    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    topicIndices.foreach { case (terms, termWeights) =>
        println("TOPIC:")
        terms.zip(termWeights).foreach { case (term, weight) =>
             println(s"${vocabArray(term.toInt)}\t$weight")
        }
        println()
    }

   
    val itemIds:RDD[String] = itemsRDD.map{ case(itemId,item)=>itemId }
    println("number of items: "+itemIds.count)
    val edges: RDD[(String,String)] = combs(itemIds,sc)
    println("number of edges: "+edges.count)
    
/*
    val edges: RDD[Edge[String]] =
      itemsRDD.map{
        x => Edge(x._1, 1, 1)
      }
    */
    //val g : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")
    val g = GraphLoader.edgeListFile(sc, dsp.graphEdgelistPath)



    // get all "user" "view" "item" events
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
