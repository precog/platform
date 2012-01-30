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
import blueeyes.json.Printer
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

// On startup
// - load config/metadata from filesystem 
// -- collect Map[ProjectionDescriptor, File]
// -- walk Map[ProjectionDescriptor, File] 
// - repair/rebuild if necessary (leave as a todo)
// - create/start various actors
// - wait for shutdown hook
// - shutdown
// -- request all actors shutdown
// -- wait for all actors to shutdown
// -- exit

class ShardServer {
  def run(config: ShardServerConfig): Unit = {
    
    val system = ActorSystem("Shard Actor System")
    
    val routingTable = new SingleColumnProjectionRoutingTable
    val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(config.metadata, ShardMetadata.dummyCheckpoints)))

    val router = system.actorOf(Props(new RoutingActor(config.baseDir, config.descriptors, routingTable, metadataActor)))
    
    val consumer = new KafkaConsumer(config.properties, router)

    val consumerThread = new Thread(consumer)
    consumerThread.start

    println("Shard Server started...")
   
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

class ShardServerConfig(val properties: Properties, val baseDir: File, val descriptors: mutable.Map[ProjectionDescriptor, File], val metadata: mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]])

object ShardServerConfig extends Logging {
  def fromFile(propsFile: File): IO[Validation[Error, ShardServerConfig]] = IOUtils.readPropertiesFile { propsFile } flatMap { fromProperties } 
  
  def fromProperties(props: Properties): IO[Validation[Error, ShardServerConfig]] = {
    val baseDir = extractBaseDir { props }
    loadDescriptors { baseDir } flatMap { desc => loadMetadata(desc) map { _.map { new ShardServerConfig(props, baseDir, desc, _) } } }
  }

  def extractBaseDir(props: Properties): File = new File(props.getProperty("querio.storage.root", "."))

  def loadDescriptors(baseDir: File): IO[mutable.Map[ProjectionDescriptor, File]] = {

    def loadMap(baseDir: File) = {
      IOUtils.walkSubdirs(baseDir).map{ _.foldLeft( mutable.Map[ProjectionDescriptor, File]() ){ (map, dir) =>
        println("loading: " + dir)
        LevelDBProjection.descriptorSync(dir).read match {
          case Some(dio) => dio.unsafePerformIO.fold({ t => logger.warn("Failed to restore %s: %s".format(dir, t)); map },
                                                     { pd => map + (pd -> dir) })
          case None      => map
        }
      }}
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

  def flattenValidations[A](l: Seq[Validation[Error,A]]): Validation[Error, Seq[A]] = {
    l.foldLeft[Validation[Error, List[A]]]( Success(List()) ) { (acc, el) => (acc, el) match {
      case (Success(ms), Success(m)) => Success(ms :+ m)
      case (Failure(e1), Failure(e2)) => Failure(e1 |+| e2)
      case (_          , Failure(e)) => Failure(e)
    }}
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
    writer.println(s)
    writer.close
  }

}

object ShardServer {
  def main(args: Array[String]) = loadConfig { args(0) } map { runServerOrDie } unsafePerformIO

  def runServerOrDie(validation: Validation[Error, ShardServerConfig]) {
    validation match {
      case Success(config) => new ShardServer run config
      case Failure(e)      => println("Error loading server config: " + e)
    }
  }

  def loadConfig(filename: String) = ShardServerConfig.fromFile { new File(filename) }
}

// vim: set ts=4 sw=4 et:
