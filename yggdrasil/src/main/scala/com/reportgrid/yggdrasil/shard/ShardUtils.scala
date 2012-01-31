package com.reportgrid.yggdrasil 
package shard

import akka.actor.{IO => _, _}

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.Await

import akka.util.Timeout
import akka.util.duration._
import akka.util.Duration

import java.io._
import java.util.Properties

import com.reportgrid.common._
import com.reportgrid.common.util.RealisticIngestMessage
import com.reportgrid.yggdrasil.kafka._

import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import scala.collection.mutable

import org.scalacheck.Gen._

import scalaz._
import scalaz.std._
import scalaz.std.AllInstances._
import scalaz.syntax._
import scalaz.syntax.validation._
import scalaz.syntax.traverse._
import scalaz.syntax.semigroup._
import scalaz.effect._
import scalaz.iteratee.EnumeratorT

import com.weiglewilczek.slf4s._

class ShardLoader(val storageShardConfig: Properties) extends StorageShardModule with RealisticIngestMessage {
  def insert(count: Int, variety: Int) {
    val events = containerOfN[List, Event](variety, genEvent).sample.get
    
    val finalEvents = 0.until(count).map(_ => events).flatten

    finalEvents.zipWithIndex.foreach {
      case (ev, idx) => storageShard.router ! EventMessage(0, idx, ev) 
    }
    
    println("Insert total: " + finalEvents.size)
  }
}

object ShardLoader extends Logging {

  def main(args: Array[String]) {

    val props = new Properties
    props.setProperty("querio.storage.root", args(0))

    val shardLoader = new ShardLoader(props)
    logger.info("Shard server - Starting")
    Await.result(shardLoader.storageShard.start, 300 seconds)
    logger.info("Shard server - Started")

    val insert = args(1).toInt
    val variety = args(2).toInt

    shardLoader.insert(insert, variety)
    
    logger.info("Shard server - Stopping")
    Await.result(shardLoader.storageShard.stop, 300 seconds) 
    logger.info("Shard server - Stopped")
  }

  def readProperties(filename: String) = {
    val props = new Properties
    props.load(new FileReader(filename))
    props
  }
}

object ShardDemoUtil {

  def main(args: Array[String]) {
    val default = if(args.length > 0) args(0) else "/tmp/test/"
    val props = new Properties
    props.setProperty("querio.storage.root", default)
    bootstrapTest(props).unsafePerformIO.map( t => println(t.descriptors + "|" + t.metadata) )
  }

  def bootstrapTest(properties: Properties): IO[Validation[Error, ShardConfig]] =
    ShardConfig.fromProperties(properties)

  def writeDummyShardMetadata() {
    val md = ShardMetadata.dummyProjections
   
    val rawEntries = 0.until(md.size) zip md.toSeq

    val descriptors = rawEntries.foldLeft( mutable.Map[ProjectionDescriptor, File]() ) {
      case (acc, (idx, (pd, md))) => acc + (pd -> new File("/tmp/test/desc"+idx))
    }

    val metadata = rawEntries.foldLeft( mutable.Map[ProjectionDescriptor, Seq[mutable.Map[MetadataType, Metadata]]]() ) {
      case (acc, (idx, (pd, md))) => acc + (pd -> md)
    }

    writeAll(descriptors, metadata).unsafePerformIO
  }

  def writeAll(descriptors: mutable.Map[ProjectionDescriptor, File], metadata: mutable.Map[ProjectionDescriptor,Seq[mutable.Map[MetadataType, Metadata]]]): IO[Unit] = 
    writeDescriptors(descriptors) unsafeZip writeMetadata(descriptors, metadata) map { _ => () }

  def writeDescriptors(descriptors: mutable.Map[ProjectionDescriptor, File]): IO[Unit] = 
    descriptors.map {
      case (pd, f) => {
        if(!f.exists) f.mkdirs
        IOUtils.writeToFile(Printer.pretty(Printer.render(pd.serialize)), new File(f, "projection_descriptor.json"))
      }
    }.toList.sequence[IO,Unit].map { _ => () }

  def writeMetadata(descriptors: mutable.Map[ProjectionDescriptor, File], metadata: mutable.Map[ProjectionDescriptor,Seq[mutable.Map[MetadataType, Metadata]]]): IO[Unit] =
    metadata.map {
      case (pd, md) => {
        descriptors.get(pd).map { f =>
          if(!f.exists) f.mkdirs
          IOUtils.writeToFile(Printer.pretty(Printer.render(md.map( _.toSeq ).serialize)), new File(f, "projection_metadata.json"))
        }.getOrElse(IO { () })
      }
    }.toList.sequence[IO,Unit].map { _ => () }
}

