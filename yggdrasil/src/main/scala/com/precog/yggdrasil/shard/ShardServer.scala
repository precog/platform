package com.precog.yggdrasil 
package shard 

import com.precog.analytics.Path
import com.precog.common._
import com.precog.util._
import com.precog.util.Bijection._
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

class ShardServer(val storageShardConfig: Properties) extends StorageShardModule 

object ShardServer extends Logging { 

  def main(args: Array[String]) {
    val shardServer = new ShardServer(readProperties(args(0)))
    val run = for (storageShard <- shardServer.storageShard) yield {
      logger.info("Shard server - Starting")
      Await.result(storageShard.start, 300 seconds)
      logger.info("Shard server - Started")

      Runtime.getRuntime.addShutdownHook(new Thread() {
        override def run() { 
          logger.info("Shard server - Stopping")
          Await.result(storageShard.stop, 300 seconds) 
          logger.info("Shard server - Stopped")
        }
      })
    }

    run.unsafePerformIO
  }

  def readProperties(filename: String) = {
    val props = new Properties
    props.load(new FileReader(filename))
    props
  }
}

trait StorageShardModule {

  def storageShardConfig: Properties

  lazy val storageShard: IO[StorageShard] = {
    ShardConfig.fromProperties(storageShardConfig) map {
      case Success(config) => new FilesystemBootstrapStorageShard(config)
      case Failure(e) => sys.error("Error loading shard config: " + e)
    }
  }
}

trait StorageShard {
  def start: Future[Unit]
  def stop: Future[Unit]

  def metadata: StorageMetadata 
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection]
}

class FilesystemBootstrapStorageShard(config: ShardConfig) extends StorageShard {
  
  lazy val system = ActorSystem("storage_shard")
  lazy implicit val dispatcher = system.dispatcher
  lazy val dbLayout = new DBLayout(config.baseDir, config.descriptors)

  lazy val routingTable = SingleColumnProjectionRoutingTable 

  lazy val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(config.metadata, config.checkpoints)), "metadata")

  lazy val metadata: StorageMetadata = new ShardMetadata(metadataActor, dispatcher) 

  lazy val router: ActorRef = system.actorOf(Props(new RoutingActor(metadataActor, routingTable, dbLayout.descriptorLocator, dbLayout.descriptorIO)), "router")

  lazy val consumer = new KafkaConsumer(config.properties, router)

  def start: Future[Unit] = Future {
    if(config.properties.getProperty("precog.kafka.enable", "false").trim.toLowerCase == "true") {
      val consumerThread = new Thread(consumer)
      consumerThread.start
    }
  }

  def stop: Future[Unit] = {
    val metadataSerializationActor: ActorRef = system.actorOf(Props(new MetadataSerializationActor(dbLayout.metadataIO, dbLayout.checkpointIO)), "metadata_serializer")

    val duration = 300 seconds
    implicit val timeout: Timeout = duration 

    gracefulStop(router, duration)(system) flatMap
      { _ => metadataActor ? FlushMetadata(metadataSerializationActor) }  flatMap
      { _ => metadataActor ? FlushCheckpoints(metadataSerializationActor) }  flatMap
      { _ => gracefulStop(metadataActor, duration)(system) }  flatMap
      { _ => gracefulStop(metadataSerializationActor, duration)(system) } map
      { _ => system.shutdown }
  }
  
  def projection(descriptor: ProjectionDescriptor)(implicit timeout: Timeout): Future[Projection] = {
    (router ? ProjectionActorRequest(descriptor)).mapTo[ValidationNEL[Throwable, ActorRef]] flatMap {
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


class ShardConfig(val properties: Properties, val baseDir: File, val descriptors: Map[ProjectionDescriptor, File], val metadata: mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]], val checkpoints: mutable.Map[Int, Int])

object ShardConfig extends Logging {
  def fromFile(propsFile: File): IO[Validation[Error, ShardConfig]] = IOUtils.readPropertiesFile { propsFile } flatMap { fromProperties } 
  
  def fromProperties(props: Properties): IO[Validation[Error, ShardConfig]] = {
    val baseDir = extractBaseDir { props }
    loadDescriptors(baseDir) flatMap { desc => loadMetadata(desc) map { _.map { meta => (desc, meta) } } } flatMap { tv => tv match {
      case Success(t) => loadCheckpoints(baseDir) map { _.map( new ShardConfig(props, baseDir, t._1, t._2, _)) }
      case Failure(e) => IO { Failure(e) }
    }}
  }

  def extractBaseDir(props: Properties): File = new File(props.getProperty("precog.storage.root", "."))

  def loadDescriptors(baseDir: File): IO[Map[ProjectionDescriptor, File]] = {

    def loadMap(baseDir: File) = 
      IOUtils.walkSubdirs(baseDir) flatMap { 
        _.foldLeft( IO(Map.empty[ProjectionDescriptor, File]) ) { (acc, dir) =>
          println("loading: " + dir)
          read(dir) flatMap {
            case Success(pd) => acc.map(_ + (pd -> dir))
            case Failure(error) => 
              logger.warn("Failed to restore %s: %s".format(dir, error))
              acc
          }
        }
      }

    def read(baseDir: File): IO[Validation[String, ProjectionDescriptor]] = IO {
      val df = new File(baseDir, "projection_descriptor.json")
      if (!df.exists) Failure("Unable to find serialized projection descriptor in " + baseDir)
      else {
        val reader = new FileReader(df)
        try {
          { (err: Extractor.Error) => err.message } <-: JsonParser.parse(reader).validated[ProjectionDescriptor]
        } finally {
          reader.close
        }
      }
    }

    loadMap(baseDir)
  }

  type MetadataSeq = Seq[mutable.Map[MetadataType, Metadata]]
  
  def loadMetadata(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, mutable.Map[ProjectionDescriptor, MetadataSeq]]] = {

    type MetadataTuple = (ProjectionDescriptor, MetadataSeq)

    def readAll(descriptors: Map[ProjectionDescriptor, File]): IO[Validation[Error, Seq[MetadataTuple]]] = {
      val validatedEntries = descriptors.toList.map{ case (d, f) => readSingle(f) map { _.map((d, _)) } }.sequence[IO, Validation[Error, (ProjectionDescriptor, MetadataSeq)]]

      validatedEntries.map(flattenValidations)
    }

    def readSingle(dir: File): IO[Validation[Error, MetadataSeq]] = {
      import JsonParser._
      val metadataFile = new File(dir, "projection_metadata.json")
      IOUtils.readFileToString(metadataFile).map { _ match {
        case None    => Success(List(mutable.Map[MetadataType, Metadata]()))
        case Some(c) => {
          val validatedTuples = parse(c).validated[List[List[(MetadataType, Metadata)]]]
          validatedTuples.map( _.map( mutable.Map(_: _*)))
        }
      }}
    }

    readAll(descriptors).map { _.map { mutable.Map(_: _*) } }
  }

  def loadCheckpoints(baseDir: File): IO[Validation[Error, mutable.Map[Int, Int]]] = {
    import JsonParser._
    val checkpointFile = new File(baseDir, "checkpoints.json")
    IOUtils.readFileToString(checkpointFile).map { _ match { 
      case None    => Success(mutable.Map[Int, Int]())
      case Some(c) => parse(c).validated[List[(Int, Int)]].map( mutable.Map(_: _*))
    }}
  }

  def flattenValidations[A](l: Seq[Validation[Error,A]]): Validation[Error, Seq[A]] = {
    l.foldLeft[Validation[Error, List[A]]]( Success(List()) ) { (acc, el) => (acc, el) match {
      case (Success(ms), Success(m)) => Success(ms :+ m)
      case (Failure(e1), Failure(e2)) => Failure(e1 |+| e2)
      case (_          , Failure(e)) => Failure(e)
    }}
  }
}

class KafkaConsumer(props: Properties, router: ActorRef) extends Runnable {
  private lazy val consumer = initConsumer

  def initConsumer = {
    val config = new ConsumerConfig(props)
    Consumer.create(config)
  }

  def run {
    val rawEventsTopic = props.getProperty("precog.storage.topic.raw", "raw")

    val streams = consumer.createMessageStreams(Map(rawEventsTopic -> 1))


    for(rawStreams <- streams.get(rawEventsTopic); stream <- rawStreams; message <- stream) {
      router ! IngestMessageSerialization.readMessage(message.buffer) 
    }
  }

  def requestStop {
    consumer.shutdown
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

object IOUtils {

  val dotDirs = "." :: ".." :: Nil
  
  def isNormalDirectory(f: File) = f.isDirectory && !dotDirs.contains(f.getName) 

  def walkSubdirs(root: File): IO[Seq[File]] =
    IO { if(!root.isDirectory) List.empty else root.listFiles.filter( isNormalDirectory ) }

  def readFileToString(f: File): IO[Option[String]] = {
    def readFile(f: File): String = {
      val in = scala.io.Source.fromFile(f)
      val content = in.mkString
      in.close
      content
    }
    IO { if(f.exists && f.canRead) Some(readFile(f)) else None }
  }

  def readPropertiesFile(s: String): IO[Properties] = readPropertiesFile { new File(s) } 
  
  def readPropertiesFile(f: File): IO[Properties] = IO {
    val props = new Properties
    props.load(new FileReader(f))
    props
  }

  def writeToFile(s: String, f: File): IO[Unit] = IO {
    val writer = new PrintWriter(new PrintWriter(f))
    try {
      writer.println(s)
    } finally { 
      writer.close
    }
  }

  def safeWriteToFile(s: String, f: File): IO[Validation[Throwable, Unit]] = IO {
    Validation.fromTryCatch {
      val tmpFile = File.createTempFile(f.getName, ".tmp", f.getParentFile)
      writeToFile(s, tmpFile).unsafePerformIO
      tmpFile.renameTo(f) // TODO: This is only atomic on POSIX systems
      Success(())
    }
  }

}

// vim: set ts=4 sw=4 et:
