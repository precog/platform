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

object ShardLoader extends RealisticIngestMessage {

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

  def main(args: Array[String]) {
    if(args.length < 2) sys.error("Must provide path and number of events to generate.")
    val base = new File(args(0))
    val count = args(1).toInt
    
    val events = containerOfN[List, Event](1, genEvent).sample.get

    load(base, events, count)
  }

  def load(baseDir: File, events: List[Event], count: Int) {
    baseDir.mkdirs

    println("Paths: " + events(0).content.size)

    val system = ActorSystem("shard_loader")

    implicit val dispatcher = system.dispatcher

    val dbLayout = new DBLayout(baseDir, mutable.Map())

    val routingTable = new SingleColumnProjectionRoutingTable
    val metadataActor: ActorRef = system.actorOf(Props(new ShardMetadataActor(mutable.Map(), mutable.Map(), dbLayout.metadataIO, dbLayout.checkpointIO)))

    val router = system.actorOf(Props(new RoutingActor(metadataActor, routingTable, dbLayout.descriptorLocator, dbLayout.descriptorIO)))
    
    implicit val timeout: Timeout = 30 seconds 
   
    val finalEvents = 0.until(count).map(_ => events).flatten

    finalEvents.zipWithIndex.foreach {
      case (ev, idx) => router ! EventMessage(0, idx, ev) 
    }

    println("Initiating shutdown")

    Await.result(gracefulStop(router, 300 seconds)(system) flatMap { _ =>  metadataActor ! FlushMetadata; gracefulStop(metadataActor, 300 seconds)(system)}, 300 seconds) 

    println("Actors stopped")
    
    println("Insert total: " + finalEvents.size)

    system.shutdown
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

