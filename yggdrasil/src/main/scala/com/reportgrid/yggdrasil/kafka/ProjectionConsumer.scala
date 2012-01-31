package com.reportgrid.yggdrasil
package kafka

import com.reportgrid.util._
import com.reportgrid.common.util.FixMe._
import leveldb._
import shard._
import Bijection._

import com.reportgrid.common._

import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import _root_.kafka.consumer._

import java.io.File
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import scala.collection._

import scalaz._
import scalaz.syntax.std.booleanV._
import scalaz.syntax.std.optionV._
import scalaz.syntax.validation._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT
import scalaz.MonadPartialOrder._

case object Stop

class RoutingActor(baseDir: File, descriptors: mutable.Map[ProjectionDescriptor, File], routingTable: RoutingTable, metadataActor: ActorRef) extends Actor with Logging {
  def syncDescriptors : IO[List[Validation[Throwable,Unit]]] = {
    projectionActors.keys.foldLeft(IO(List[Validation[Throwable,Unit]]())) { 
      case (io,descriptor) => {
        descriptors.get(descriptor).map { dir =>
          io.flatMap { a => LevelDBProjection.descriptorSync(dir).sync(descriptor).map(_ :: a) }
        } getOrElse {
          io.map(a => (new RuntimeException("Could not locate directory for " + descriptor) : Throwable).fail[Unit] :: a)
        }
      }
    }
  }

  def dirFor(descriptor : ProjectionDescriptor) : File = {
    descriptors.get(descriptor) match {
      case Some(dir) => dir
      case None => {
        val newDir = File.createTempFile("col", "", baseDir)
        newDir.delete
        newDir.mkdirs
        logger.debug(newDir.toString)
        LevelDBProjection.descriptorSync(newDir).sync(descriptor).unsafePerformIO.fold({ t => logger.error("Failed to sync descriptor: " + t) },
                                                                                       { _ => ()})
        descriptors += (descriptor -> newDir)
        newDir
      }
    }
  }

  private val pathDelimeter = "//"
  private val partDelimeter = "-"

  def toPath(columns: Seq[ColumnDescriptor]): String = {
    columns.map( s => sanitize(s.path.toString)).mkString(pathDelimeter) + pathDelimeter +
    columns.map( s => sanitize(s.selector.toString) + partDelimeter + sanitize(s.valueType.toString)).mkString(partDelimeter)
  }

  def projectionSuffix(descriptor: ProjectionDescriptor): String = ""

  private val whitespace = "//W".r

  def sanitize(s: String) = whitespace.replaceAllIn(s, "_") 
      
  val projectionActors = Cache.concurrent[ProjectionDescriptor, ActorRef](
    CacheSettings(
      expirationPolicy = ExpirationPolicy(None, None, TimeUnit.SECONDS), 
      evict = { 
        (descriptor, actor) => {
          val sync = LevelDBProjection.descriptorSync(descriptors(descriptor))
          sync.sync(descriptor).map(_ => actor ! Stop).unsafePerformIO
        }
      }
    )
  )

  // Event processing
  // 
  // - break event into route actions (note it is possible for some data not to be routed and other data to be routed to multiple locations)
  // - send count of route actions to metadata layer for sync accounting
  // - for each route action
  // -- Determine required projection
  // -- Lookup or create the required projection
  // --- This includes creation of initial metadata
  // -- Augment the route action with value based metadata
  // -- Send route action

  private implicit val timeout: Timeout = 5 seconds

  def receive = {
    case SyncMessage(producerId, syncId, eventIds) => //TODO

    case em @ EventMessage(pid, eid, ev @ Event(_, data)) =>
      val unpacked = RoutingTable.unpack(ev)
      val qualifiedSelectors = unpacked filter { 
        case Some(_) => true
        case _       => false
      } map { 
        case Some(x) => x
        case _       => sys.error("Theoretically unreachable code")
      }
      val projectionUpdates = routingTable.route(qualifiedSelectors)

      registerCheckpointExpectation(pid, eid, projectionUpdates.size)

      for {
        (descriptor, values) <- projectionUpdates 
      } {
        val comparator = ProjectionComparator.forProjection(descriptor)
        val actor: ValidationNEL[Throwable, ActorRef] = projectionActors.get(descriptor).toSuccess(new RuntimeException("No cached actor available."): Throwable).toValidationNel.orElse {
          LevelDBProjection(dirFor(descriptor), descriptor, Some(comparator)).map(p => context.actorOf(Props(new ProjectionActor(p, descriptor))))
        }

        actor match {
          case Success(actor) =>
            projectionActors.putIfAbsent(descriptor, actor)
            val fut = actor ? ProjectionInsert(em.uid, values)
            fut.onComplete { _ => 
              fixme("Ignoring metadata update for now, but need to come back.")
              //metadataActor ! UpdateMetadata(pid, eid, descriptor, values, extractMetadataFor(descriptor, boundMetadata))
            }

          case Failure(errors) => 
            for (t <- errors.list) logger.error("Could not obtain actor for projection: " , t)
        }
      }
  }

  def registerCheckpointExpectation(pid: Int, eid: Int, count: Int): Unit = metadataActor ! ExpectedEventActions(pid, eid, count)

//  def extractMetadataFor(desc: ProjectionDescriptor, metadata: Set[(ColumnDescriptor, JValue)]): Seq[Set[Metadata]] = 
//    desc.columns flatMap { c => metadata.exists(_ == c).option(c.metadata) } toSeq
}

case class ProjectionInsert(id: Long, values: Seq[JValue])

case class ProjectionGet(idInterval : Interval[Identities], sender : ActorRef)

trait ProjectionResults {
  val desc : ProjectionDescriptor
  def enumerator : EnumeratorT[Unit, Seq[CValue], IO]
}

class ProjectionActor(projection: LevelDBProjection, descriptor: ProjectionDescriptor) extends Actor {

  def asCValue(jval: JValue): CValue = jval match { 
    case JString(s) => CString(s)
    case JInt(i) => CNum(BigDecimal(i))
    case JDouble(d) => CDouble(d)
    case JBool(b) => CBoolean(b)
    case JNull => CNull
    case x       => sys.error("JValue type not yet supported: " + x.getClass.getName )
  }

  def receive = {
    case Stop => //close the db
      projection.close.unsafePerformIO

    case ProjectionInsert(id, values) => 
      projection.insert(Vector(id), values.map(asCValue)).unsafePerformIO

    case ProjectionGet(interval, sender) =>
      sender ! new ProjectionResults {
        val desc = descriptor
        def enumerator = projection.getValuesByIdRange[Unit](interval).apply[IO]
      }
  }
}

// vim: set ts=4 sw=4 et:
