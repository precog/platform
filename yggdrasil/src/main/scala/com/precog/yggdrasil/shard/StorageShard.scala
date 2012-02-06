package com.precog.yggdrasil 
package shard 

import com.precog.analytics.Path
import com.precog.common._
import com.precog.util._
import com.precog.util.Bijection._
import com.precog.yggdrasil.util.IOUtils
import kafka._
import leveldb._

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.dispatch.Await
import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.pattern.ask
import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration
import akka.actor.Terminated
import akka.actor.ReceiveTimeout
import akka.actor.ActorTimeoutException

import blueeyes.util._
import blueeyes.json.Printer._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._
import _root_.kafka.consumer._

import java.io.File
import java.io.FileReader
import java.io.PrintWriter
import java.nio.ByteBuffer
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.scalacheck.Gen._

import com.precog.common._
import com.precog.common.Event
import com.precog.common.util.RealisticIngestMessage

import scala.collection.mutable
import scala.annotation.tailrec

import scalaz._
import scalaz.std._
import scalaz.std.AllInstances._
import scalaz.syntax._
import scalaz.syntax.validation._
import scalaz.syntax.traverse._
import scalaz.syntax.semigroup._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.biFunctor
import scalaz.Scalaz._


trait StorageShard {
  def start: Future[Unit]
  def stop: Future[Unit]

  def metadata: StorageMetadata 
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection]
}

trait ShardLogging { this: Logging =>
  def lpre(msg: String): String = "[Storage Shard] %s".format(msg)
  def li(m: String) = (_:Any) => logger.info(lpre(m))
  def ld(m: String) = (_:Any) => logger.debug(lpre(m))
  def le(m: String) = (_:Any) => logger.error(lpre(m))
  def lee(m: String, t: Throwable) = (_:Any) => logger.error(lpre(m), t)
  def rle(m: String): PartialFunction[Throwable, Unit] = { case t => lee(m, t)(()) }
}

trait FilesystemBootstrapStorageShard extends StorageShard with Logging with ShardLogging {
  def shardConfig: ShardConfig

  lazy implicit val system = ActorSystem("storage_shard")
  lazy implicit val executionContext = ExecutionContext.defaultExecutionContext
  lazy implicit val dispatcher = system.dispatcher

  lazy val dbLayout = new DBLayout(shardConfig.baseDir, shardConfig.descriptors)

  lazy val routingTable: RoutingTable = SingleColumnProjectionRoutingTable 
  lazy val routingActor: ActorRef = system.actorOf(Props(new RoutingActor(metadataActor, routingTable, dbLayout.descriptorLocator, dbLayout.descriptorIO)), "router")
  lazy val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(shardConfig.metadata, shardConfig.checkpoints)), "metadata")
  lazy val metadata: StorageMetadata = new ShardMetadata(metadataActor, dispatcher) 

  def testLoad() {
    def parse(s: String): Option[(Int, Int)] = {
      if(s == null) None
      else {
        val parts = s.split(",")
        if(parts.length != 2) None
        else Some((parts(0).toInt, parts(1).toInt))
      }
    }

    parse(shardConfig.properties.getProperty("precog.test.load.dummy")) foreach { 
      case (count, variety) => {
        println(lpre("Dummy data load (%d,%d)".format(count, variety)))
        SimpleShardLoader.load(routingActor, count, variety) 
      }
    }
  }

  def start: Future[Unit] = {
    Future(testLoad())
  }

  def stop: Future[Unit] = {
    val metadataSerializationActor: ActorRef = system.actorOf(Props(new MetadataSerializationActor(dbLayout.metadataIO, dbLayout.checkpointIO)), "metadata_serializer")

    val defaultSystem = system
    val defaultTimeout = 300 seconds
    implicit val timeout: Timeout = defaultTimeout

    def actorStop(actor: ActorRef, timeout: Duration = defaultTimeout, system: ActorSystem = defaultSystem) = (_: Any) =>
      gracefulStop(actor, timeout)(system)

    def flushMetadata = (_: Any) => metadataActor ? FlushMetadata(metadataSerializationActor)
    def flushCheckpoints = (_: Any) => metadataActor ? FlushCheckpoints(metadataSerializationActor)
  
    Future { li("Stopping")(_) } map 
      ld("Stopping routing actor") flatMap
      actorStop(routingActor) recover rle("Error stopping routing actor") map
      ld("Flushing metadata") flatMap
      flushMetadata recover rle("Error flushing metadata") map
      ld("Flushing checkpoints") flatMap
      flushCheckpoints recover rle("Error flushing checkpoints") map
      ld("Stopping metadata actor") flatMap
      actorStop(metadataActor) recover rle("Error stopping metadata actor") map
      ld("Stopping flush actor") flatMap
      actorStop(metadataSerializationActor) recover rle("Error stopping flush actor") map
      ld("Stopping actor system") map
      { _ => system.shutdown } recover rle("Error stopping actor system") map
      li("Stopped")
  }

  def store(msg: EventMessage): IO[Unit] = IO {
    routingActor ! msg
  }
  
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection] = {
    (routingActor ? ProjectionActorRequest(descriptor)).mapTo[ValidationNEL[Throwable, ActorRef]] flatMap {
      case Success(actorRef) => (actorRef ? ProjectionGet).mapTo[Projection]
    }
  }

  def gracefulStop(target: ActorRef, timeout: Duration)(implicit system: ActorSystem): Future[Boolean] = {
    if (target.isTerminated) {
      Promise.successful(true)
    } else {
      val result = Promise[Boolean]()
      system.actorOf(Props(new Actor {
        // Terminated will be received when target has been stopped
        context watch target
        target ! PoisonPill
        // ReceiveTimeout will be received if nothing else is received within the timeout
        context setReceiveTimeout timeout

        def receive = {
          case Terminated(a) if a == target ⇒
            result success true
            context stop self
          case ReceiveTimeout ⇒
            result failure new ActorTimeoutException(
              "Failed to stop [%s] within [%s]".format(target.path, context.receiveTimeout))
            context stop self
        }
      }))
      result
    }
  }
}

class DBLayout(baseDir: File, descriptorLocations: Map[ProjectionDescriptor, File]) { 
  private var descriptorDirs = descriptorLocations

  val descriptorName = "projection_descriptor.json"
  val metadataName = "projection_metadata.json"
  val checkpointName = "checkpoints.json"

  def newRandomDir(parent: File): File = {
    val newDir = File.createTempFile("col", "", parent)
    newDir.delete
    newDir.mkdirs
    newDir
  }

  def newDescriptorDir(descriptor: ProjectionDescriptor, parent: File): File = newRandomDir(parent)

  val descriptorLocator = (descriptor: ProjectionDescriptor) => IO {
    descriptorDirs.get(descriptor) match {
      case Some(x) => x
      case None    => {
        val newDir = newDescriptorDir(descriptor, baseDir)
        descriptorDirs += (descriptor -> newDir)
        newDir
      }
    }
  }

  val descriptorIO = (descriptor: ProjectionDescriptor) =>
    descriptorLocator(descriptor).map( d => new File(d, descriptorName) ).flatMap {
      f => IOUtils.safeWriteToFile(pretty(render(descriptor.serialize)), f)
    }.map(_ => ())

  val metadataIO = (descriptor: ProjectionDescriptor, metadata: Seq[MetadataMap]) => {
    descriptorLocator(descriptor).map( d => new File(d, metadataName) ).flatMap {
      f => IOUtils.safeWriteToFile(pretty(render(metadata.toList.map( _.toList).serialize)), f)
    }.map(_ => ())
  }

  val checkpointIO = (checkpoints: Checkpoints) => {
    IOUtils.safeWriteToFile(pretty(render(checkpoints.toList.serialize)), new File(baseDir, checkpointName)).map(_ => ())
  }
}
