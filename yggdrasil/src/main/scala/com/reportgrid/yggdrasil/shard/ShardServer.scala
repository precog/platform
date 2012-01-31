/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.reportgrid.yggdrasil 
package shard 

import com.reportgrid.analytics.Path
import com.reportgrid.common._
import com.reportgrid.util._
import com.reportgrid.util.Bijection._
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

import com.reportgrid.common._
import com.reportgrid.common.Event
import com.reportgrid.common.util.RealisticIngestMessage

import scala.collection._
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

object ShardServer { 

  def main(args: Array[String]) {
    val props = new Properties
    props.load(new FileReader(args(0)))
    start(props)

    // need to register shutdown hooks and implement
    // stop behavior
  }

  def start(props: Properties) {
    ShardConfig.fromProperties(props).unsafePerformIO match {
      case Success(config) => internalStart(config)
      case Failure(e) => sys.error("Error loading shard config: " + e)
    }
  }

  def internalStart(config: ShardConfig): Unit = {
    val system = ActorSystem("storage_shard")

    val dbLayout = new DBLayout(config.baseDir, config.descriptors) 
    
    val routingTable = new SingleColumnProjectionRoutingTable
    val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(config.metadata, config.checkpoints)))

    val router = system.actorOf(Props(new RoutingActor(metadataActor, routingTable, dbLayout.descriptorLocator, dbLayout.descriptorIO)))
    
    val consumer = new KafkaConsumer(config.properties, router)

    val consumerThread = new Thread(consumer)
    consumerThread.start

    println("Shard Server started...")
  }
}

trait StorageShardModule {

  def storageShardConfig: Properties

  def storageShard: StorageShard
}

trait StorageShard {
  def start: Future[Unit]

  def stop: Future[Unit]
}

class ShardConfig(val properties: Properties, val baseDir: File, val descriptors: mutable.Map[ProjectionDescriptor, File], val metadata: mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]], val checkpoints: mutable.Map[Int, Int])

object ShardConfig extends Logging {
  def fromFile(propsFile: File): IO[Validation[Error, ShardConfig]] = IOUtils.readPropertiesFile { propsFile } flatMap { fromProperties } 
  
  def fromProperties(props: Properties): IO[Validation[Error, ShardConfig]] = {
    val baseDir = extractBaseDir { props }
    loadDescriptors { baseDir } flatMap { desc => loadMetadata(desc) map { _.map { meta => (desc, meta) } } } flatMap { tv => tv match {
      case Success(t) => loadCheckpoints(baseDir) map { _.map( new ShardConfig(props, baseDir, t._1, t._2, _)) }
      case Failure(e) => IO { Failure(e) }
    }}
   
  }

  def extractBaseDir(props: Properties): File = new File(props.getProperty("querio.storage.root", "."))

  def loadDescriptors(baseDir: File): IO[mutable.Map[ProjectionDescriptor, File]] = {

    def loadMap(baseDir: File) = {
      IOUtils.walkSubdirs(baseDir).map{ _.foldLeft( mutable.Map[ProjectionDescriptor, File]() ){ (map, dir) =>
        println("loading: " + dir)
        read(dir) match {
          case Some(dio) => dio.unsafePerformIO.fold({ t => logger.warn("Failed to restore %s: %s".format(dir, t)); map },
                                                     { pd => map + (pd -> dir) })
          case None      => map
        }
      }}
    }

    def read(baseDir: File): Option[IO[Validation[String, ProjectionDescriptor]]] = {
      val df = new File(baseDir, "projection_descriptor.json")

      if (df.exists) Some {
        IO {
          val reader = new FileReader(df)
          try {
            { (err: Extractor.Error) => err.message } <-: JsonParser.parse(reader).validated[ProjectionDescriptor]
          } finally {
            reader.close
          }
        }
      } else {
        None
      }
    }
    loadMap(baseDir)
  }

  type MetadataSeq = Seq[mutable.Map[MetadataType, Metadata]]
  
  def loadMetadata(descriptors: mutable.Map[ProjectionDescriptor, File]): IO[Validation[Error, mutable.Map[ProjectionDescriptor, MetadataSeq]]] = {

    type MetadataTuple = (ProjectionDescriptor, MetadataSeq)

    def readAll(descriptors: mutable.Map[ProjectionDescriptor, File]): IO[Validation[Error, Seq[MetadataTuple]]] = {
      val validatedEntries = descriptors.toList.map{ case (d, f) => readSingle(f) map { _.map((d, _)) } }.sequence[IO, Validation[Error, (ProjectionDescriptor, MetadataSeq)]]

      validatedEntries.map(flattenValidations)
    }

    def readSingle(dir: File): IO[Validation[Error, MetadataSeq]] = {
      import JsonParser._
      val metadataFile = new File(dir, "projection_metadata.json")
      IOUtils.readFileToString(metadataFile).map { content => 
        val validatedTuples = parse(content.getOrElse("")).validated[List[List[(MetadataType, Metadata)]]]
        validatedTuples.map( _.map( mutable.Map(_: _*)))
      }
    }

    readAll(descriptors).map { _.map { mutable.Map(_: _*) } }
  }

  def loadCheckpoints(baseDir: File): IO[Validation[Error, mutable.Map[Int, Int]]] = {
    import JsonParser._
    val checkpointFile = new File(baseDir, "checkpoints.json")
    IOUtils.readFileToString(checkpointFile).map { content =>
      parse(content.getOrElse("")).validated[List[(Int, Int)]].map( mutable.Map(_: _*))
    }
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
    val rawEventsTopic = props.getProperty("querio.storage.topic.raw", "raw")

    val streams = consumer.createMessageStreams(Map(rawEventsTopic -> 1))


    for(rawStreams <- streams.get(rawEventsTopic); stream <- rawStreams; message <- stream) {
      router ! IngestMessageSerialization.readMessage(message.buffer) 
    }
  }

  def requestStop {
    consumer.shutdown
  }
}

class DBLayout(baseDir: File, descriptorDirs: mutable.Map[ProjectionDescriptor, File]) { 

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
        descriptorDirs.put(descriptor, newDir)
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
