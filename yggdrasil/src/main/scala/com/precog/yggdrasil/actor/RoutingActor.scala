package com.precog
package yggdrasil
package actor 

import com.precog.common._

import akka.actor.Actor
import akka.actor.Scheduler
import akka.actor.ActorRef
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import blueeyes.json.JsonAST._

import com.weiglewilczek.slf4s._

import annotation.tailrec
import collection.mutable
import java.util.concurrent.atomic.AtomicLong

import scalaz._

object RoutingActor {
  private val pathDelimeter = "//"
  private val partDelimeter = "-"

  def toPath(columns: Seq[ColumnDescriptor]): String = {
    columns.map( s => sanitize(s.path.toString)).mkString(pathDelimeter) + pathDelimeter +
    columns.map( s => sanitize(s.selector.toString) + partDelimeter + sanitize(s.valueType.toString)).mkString(partDelimeter)
  }

  def projectionSuffix(descriptor: ProjectionDescriptor): String = ""

  private val whitespace = "//W".r
  def sanitize(s: String) = whitespace.replaceAllIn(s, "_") 

  private[actor] implicit val timeout: Timeout = 30 seconds
}

case object Start 
case object ControlledStop
case class AwaitShutdown(replyTo: ActorRef)

class BatchStoreActor(val store: EventStore, val idleDelayMillis: Long, ingestActor: Option[ActorRef], scheduler: Scheduler, shutdownCheck: Duration = Duration(1, "second")) extends Actor with ReadAheadEventIngest with Logging {

  private val initiated = new AtomicLong(0)
  private val processed = new AtomicLong(0)

  def receive = {
    case Status =>
      sender ! status()
    case Start =>
      start
      sender ! ()
    case ControlledStop =>
      initiateShutdown
      self ! AwaitShutdown(sender)

    case as @ AwaitShutdown(replyTo) =>
      val pending = pendingBatches
      if(pending <= 0) {
        logger.debug("Routing actor shutdown - Complete")
        replyTo ! ()
      } else {
        logger.debug("Routing actor shutdown - Pending batches (%d)".format(pending)) 
        scheduler.scheduleOnce(shutdownCheck, self, as)
      }
    case r @ NoIngestData    => processResult(r) 
    case r @ IngestErrors(_) => processResult(r) 
    case r @ IngestData(_)   => processResult(r)
    case DirectIngestData(d) =>
      initiated.incrementAndGet
      processResult(IngestData(d)) 
      sender ! ()
    case m                   => logger.error("Unexpected ingest actor message: " + m.getClass.getName) 
  }

  def status(): JValue = JObject.empty ++ JField("Routing", JObject.empty ++
      JField("initiated", JInt(initiated.get)) ++ JField("processed", JInt(initiated.get)))

  def processResult(ingestResult: IngestResult) {
    handleResult(ingestResult)
    processed.incrementAndGet
  }

  def pendingBatches(): Long = initiated.get - processed.get

  def scheduleIngestRequest(delay: Long) {
    ingestActor.map { actor =>
      initiated.incrementAndGet
      if(delay <= 0) {
        actor ! GetMessages(self)
      } else {
        scheduler.scheduleOnce(Duration(delay, "millis"), actor, GetMessages(self))
      }
    }
  }
}

trait ReadAheadEventIngest extends Logging {
  def store: EventStore
  def idleDelayMillis: Long
 
  def scheduleIngestRequest(delay: Long): Unit

  private var inShutdown = false
  private val delayIngestFollowup = GetIngestAfter(idleDelayMillis)

  val initialAction = GetIngestNow
  
  def start() {
    inShutdown = false
    handleNext(initialAction)
  }
  
  def handleNext(nextAction: IngestAction) {
    if(!inShutdown) {
      nextAction match {
        case GetIngestNow =>
          scheduleIngestRequest(0)
        case GetIngestAfter(delay) =>
          scheduleIngestRequest(delay)
        case NoIngest =>
          // noop
      }
    }
  }

  def handleResult(result: IngestResult) {
    handleNext(nextAction(result))
    process(result)
  }

  def initiateShutdown() {
    inShutdown = true
  }
  
  def nextAction(ingest: IngestResult): IngestAction = ingest match { 
    case NoIngestData => 
      delayIngestFollowup
    case IngestErrors(errors) => 
      logErrors(errors)
      delayIngestFollowup 
    case IngestData(_) => 
      GetIngestNow 
  }

  def process(ingest: IngestResult) = ingest match {
    case IngestData(messages) =>
      store.store(messages)
    case NoIngestData         =>
      // noop
    case IngestErrors(_)      =>
      // noop
  }
  
  private def logErrors(errors: Seq[String]) = errors.foreach( logger.error(_) )
}

class EventStore(routingTable: RoutingTable, projectionActors: ActorRef, metadataActor: ActorRef, batchTimeout: Duration, implicit val timeout: Timeout, implicit val execContext: ExecutionContext) extends Logging {

  type ActionMap = mutable.Map[ProjectionDescriptor, (Seq[ProjectionInsert], Seq[InsertComplete])]

  def store(events: Seq[IngestMessage]): ValidationNEL[Throwable, Unit] = {
    import scala.collection.mutable

    var actions = buildActions(events)
    val future = dispatchActions(actions)
    Await.result(future, batchTimeout)
  }

  def buildActions(events: Seq[IngestMessage]): ActionMap = {
    
    def updateActions(event: IngestMessage, actions: ActionMap): ActionMap = {
      event match {
        case SyncMessage(_, _, _) => actions 

        case em @ EventMessage(eventId, _) =>

          @tailrec
          def update(updates: Array[ProjectionData], actions: ActionMap, i: Int = 0): ActionMap = {

            def applyUpdates(update: ProjectionData, actions: ActionMap): ActionMap = {
              val insert = ProjectionInsert(update.identities, update.values)
              val complete = InsertComplete(eventId, update.descriptor, update.values, update.metadata)
              val (inserts, completes) = actions.get(update.descriptor) getOrElse { (Vector.empty, Vector.empty) }
              val newActions = (inserts :+ insert, completes :+ complete) 
              actions += (update.descriptor -> newActions)
            }

            if(i < updates.length) {
              update(updates, applyUpdates(updates(i), actions), i+1)
            } else {
              actions
            }
          }

          update(routingTable.route(em), actions)
      }
    }
  
    @tailrec
    def build(events: Seq[IngestMessage], actions: ActionMap, i: Int = 0): ActionMap = {
      if(i < events.length) {
        build(events, updateActions(events(i), actions), i+1)
      } else {
        actions
      }
    }

    build(events, mutable.Map.empty[ProjectionDescriptor, (Seq[ProjectionInsert], Seq[InsertComplete])])
  }

  def dispatchActions(actions: ActionMap): Future[ValidationNEL[Throwable, Unit]] = {
    val acquire = projectionActors ? AcquireProjectionBatch(actions.keys)
    acquire.flatMap {
      case ProjectionBatchAcquired(actorMap) =>
        val futs: List[Future[ProjectionDescriptor]] = actions.keys.map{ desc =>
          val (inserts, completes)  = actions(desc)
          val actor = actorMap(desc)
          (actorMap(desc) ? ProjectionBatchInsert(inserts)).flatMap { _ =>
            metadataActor ? UpdateMetadata(completes)
          }.map { _ => desc }
        }(collection.breakOut)

        Future.sequence[ProjectionDescriptor, List](futs).flatMap{ descs => 
          projectionActors ? ReleaseProjectionBatch(descs)
        }.map{ _ => Success(()) }

      case ProjectionError(errs) =>
        Future(Failure(errs))
    }
  }
}

case class InsertComplete(eventId: EventId, descriptor: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]])
case class InsertBatchComplete(inserts: Seq[InsertComplete])

case class DirectIngestData(messages: Seq[IngestMessage])

sealed trait IngestAction
case object NoIngest extends IngestAction
case object GetIngestNow extends IngestAction
case class GetIngestAfter(delay: Long) extends IngestAction
