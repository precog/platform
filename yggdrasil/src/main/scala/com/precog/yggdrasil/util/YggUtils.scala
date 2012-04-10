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
package com.precog.yggdrasil
package util

import org.joda.time._

import com.precog.common._
import com.precog.common.util._
import com.precog.common.kafka._
import com.precog.yggdrasil.leveldb._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File
import java.nio.ByteBuffer

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import scopt.mutable.OptionParser

import scala.collection.SortedSet
import scala.collection.SortedMap

import _root_.kafka.message._

import org.I0Itec.zkclient._
import org.apache.zookeeper.data.Stat

object YggUtils {

 
  def usage(message: String*): String = {
    val initial = message ++ List("Usage: yggutils {command} {flags/args}",""," For details on a particular command enter yggutils {command} -help", "")
    
    commands.foldLeft(initial) {
      case (acc, cmd) => acc :+ "%-20s : %s".format(cmd.name, cmd.description)
    }.mkString("\n")

  }

  val commands = List(
    DescriptorSummary,
    DumpLocalKafka,
    DumpCentralKafka,
    IngestStatus
  )

  val commandMap: Map[String, Command] = commands.map( c => (c.name, c) )(collection.breakOut)

  def main(args: Array[String]) {
    if(args.length > 0) {
      commandMap.get(args(0)).map { c =>
        c.run(args.slice(1,args.length))
      }.getOrElse {
        die(usage("Unknown command: [%s]".format(args(0))))
      }
    } else {
      die(usage())
    }
  }

  def die(msg: String, code: Int = 1) {
    println(msg)
    sys.exit(code)
  }

}

trait Command {
  def name(): String
  def description(): String
  def run(args: Array[String])
}

object DescriptorSummary extends Command {
  val name = "describe" 
  val description = "describe paths and selectors" 
  
  def run(args: Array[String]) {
    println(args.mkString(","))
    val config = new Config
    val parser = new OptionParser("ygg describe") {
      opt("p", "path", "<path>", "root data path", {p: String => config.path = Some(Path(p))})
      opt("s", "selector", "<selector>", "root object selector", {s: String => config.selector = Some(JPath(s))})
      booleanOpt("v", "verbose", "<vebose>", "show selectors as well", {v: Boolean => config.verbose = v})
      arg("<datadir>", "shard data dir", {d: String => config.dataDir = d})
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }

  implicit val tOrdering = new Ordering[(JPath, CType)] {
    def compare(a: (JPath, CType), b: (JPath, CType)): Int = {
      val jord = implicitly[Ordering[JPath]]
      val sord = implicitly[Ordering[String]] 
      val jo = jord.compare(a._1, b._1)
      if(jo != 0) {
        jo 
      } else {
        sord.compare(a._2.toString, b._2.toString)
      }
    }
  }

  def process(config: Config) {
    println("describing data at %s matching %s%s".format(config.dataDir, 
                                                         config.path.map(_.toString).getOrElse("*"),
                                                         config.selector.map(_.toString).getOrElse(".*")))
    
    show(extract(load(config.dataDir)), config.verbose)
  }

  def load(dataDir: String) = {
    val dir = new File(dataDir)
    for(d <- dir.listFiles if d.isDirectory && !(d.getName == "." || d.getName == "..")) yield {
      readDescriptor(d)
    }
  }

  def extract(descs: Array[ProjectionDescriptor]): SortedMap[Path, SortedSet[(JPath, CType)]] = {
    implicit val pord = new Ordering[Path] {
      val sord = implicitly[Ordering[String]] 
      def compare(a: Path, b: Path) = sord.compare(a.toString, b.toString)
    }
    descs.foldLeft(SortedMap[Path,SortedSet[(JPath, CType)]]()) {
      case (acc, desc) =>
       desc.columns.foldLeft(acc) {
         case (acc, ColumnDescriptor(p, s, t, _)) =>
           val update = acc.get(p) map { _ + (s -> t) } getOrElse { SortedSet((s, t)) } 
           acc + (p -> update) 
       }
    }
  }

  def show(summary: SortedMap[Path, SortedSet[(JPath, CType)]], verbose: Boolean) {
    summary.foreach { 
      case (p, sels) =>
        println(p)
        if(verbose) {
          sels.foreach {
            case (s, ct) => println("  %s -> %s".format(s, ct))
          }
          println
        }
    }
  }

  def readDescriptor(dir: File): ProjectionDescriptor = {
    val rawDescriptor = IOUtils.rawReadFileToString(new File(dir, "projection_descriptor.json"))
    JsonParser.parse(rawDescriptor).validated[ProjectionDescriptor].toOption.get
  }

  class Config(var path: Option[Path] = None, 
               var selector: Option[JPath] = None,
               var dataDir: String = ".",
               var verbose: Boolean = false)
}

object DumpLocalKafka extends Command {
  val name = "dump_local_kafka"
  val description = "Dump contents of local message file"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("ygg dump_local_kafka") {
      intOpt("s", "start", "<message-start>", "first message to dump", {s: Int => config.start = Some(s)})
      intOpt("f", "finish", "<message-finish>", "last message to dump", {f: Int => config.finish = Some(f)})
      arg("<local-kafka-file>", "local kafka message file", {d: String => config.file = d})
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }

  val eventCodec = new KafkaEventCodec

  def process(config: Config) {


    def traverse(itr: Iterator[MessageAndOffset], start: Option[Int], finish: Option[Int], i: Int = 0): Unit = {
      val beforeFinish = finish.map( _ >= i ).getOrElse(true)
      val afterStart = start.map( _ <= i ).getOrElse(true)
      if(beforeFinish && itr.hasNext) {
        val next = itr.next
        if(afterStart) {
          val event = eventCodec.toEvent(next.message)
          println("Event-%06d Path: %s Token: %s".format(i+1, event.path, event.tokenId))
          println(Printer.pretty(Printer.render(event.data)))
        }
        traverse(itr, start, finish, i+1)
      } else {
        ()
      }
    }

    val ms = new FileMessageSet(new File(config.file), false)

    traverse(ms.iterator, config.start, config.finish)
  }

  class Config(var file: String = "",
               var start: Option[Int] = None,
               var finish: Option[Int] = None)
}

object DumpCentralKafka extends Command {
  val name = "dump_central_kafka"
  val description = "Dump contents of central message file"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("ygg dump_central_kafka") {
      intOpt("s", "start", "<message-start>", "first message to dump", {s: Int => config.start = Some(s)})
      intOpt("f", "finish", "<message-finish>", "last message to dump", {f: Int => config.finish = Some(f)})
      arg("<central-kafka-file>", "central kafka message file", {d: String => config.file = d})
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }

  val eventCodec = new KafkaIngestMessageCodec

  def process(config: Config) {

    def traverse(itr: Iterator[MessageAndOffset], start: Option[Int], finish: Option[Int], i: Int = 0): Unit = {
      val beforeFinish = finish.map( _ >= i ).getOrElse(true)
      val afterStart = start.map( _ <= i ).getOrElse(true)
      if(beforeFinish && itr.hasNext) {
        val next = itr.next
        if(afterStart) {
          eventCodec.toEvent(next.message) match {
            case EventMessage(EventId(pid, sid), Event(path, tokenId, data, _)) =>
              println("Event-%06d Id: (%d/%d) Path: %s Token: %s".format(i+1, pid, sid, path, tokenId))
              println(Printer.pretty(Printer.render(data)))
            case _ =>
          }
        }
        traverse(itr, start, finish, i+1)
      } else {
        ()
      }
    }

    val ms = new FileMessageSet(new File(config.file), false)

    traverse(ms.iterator, config.start, config.finish)
  }

  class Config(var file: String = "",
               var start: Option[Int] = None,
               var finish: Option[Int] = None)
}

object IngestStatus extends Command {
  val name = "ingest_status"
  val description = "Dump details about ingest status"
  

  def run(args: Array[String]) {
    val conn = new ZkConnection("localhost:2181")
    val client = new ZkClient(conn)
    val config = new Config
    val parser = new OptionParser("ygg ingest_status") {
      intOpt("s", "limit", "<sync-limit-messages>", "if sync is greater than the specified limit an error will occur", {s: Int => config.limit = s})
      intOpt("l", "lag", "<time-lag-minutes>", "if update lag is greater than the specified value an error will occur", {l: Int => config.lag = l})
    }
    if (parser.parse(args)) {
      process(conn, client, config)
    } else { 
      parser
    }
    client.close
  }

  val shardCheckpointPath = "/beta/com/precog/ingest/v1/shard/checkpoint/shard01"
  val relayAgentPath = "/test/com/precog/ingest/v1/relay_agent/qclus-demo01"

  def process(conn: ZkConnection, client: ZkClient, config: Config) {
    val relayRaw = getJsonAt(relayAgentPath, client).getOrElse(sys.error("Error reading relay agent state"))
    val shardRaw = getJsonAt(shardCheckpointPath, client).getOrElse(sys.error("Error reading shard state"))

    val relayState = relayRaw.deserialize[EventRelayState]  
    val shardState = shardRaw.deserialize[YggCheckpoint]

    val shardValues = shardState.messageClock.map
    val pid = relayState.idSequenceBlock.producerId
    val shardSID = shardValues.get(pid).map { _.toString }.getOrElse{ "NA" }
    val relaySID = (relayState.nextSequenceId - 1).toString

    println("Messaging State")
    println("PID: %d Shard SID: %s Ingest (relay) SID: %s".format(pid, shardSID, relaySID))

    val syncDelta = relayState.nextSequenceId - 1 - shardValues.get(pid).getOrElse(0) 

    if(syncDelta > config.limit) {
      println("Message sync limit exceeded: %d > %d".format(syncDelta, config.limit))
      sys.exit(1)
    }

    val relayStat = getStatAt(relayAgentPath, conn).getOrElse(sys.error("Unable to stat relay agent state"))
    val shardStat = getStatAt(shardCheckpointPath, conn).getOrElse(sys.error("Unable to stat shard state"))

    val relayModified = new DateTime(relayStat.getMtime, DateTimeZone.UTC)
    val shardModified = new DateTime(shardStat.getMtime, DateTimeZone.UTC)

    if(isOlderThan(config.lag, relayModified)) {
      println("Relay state exceeds acceptable lag. (Last Updated: %s)".format(relayModified))
      sys.exit(2)
    }
    if(isOlderThan(config.lag, shardModified)) {
      println("Shard state exceeds acceptable lag. (Last updated: %s)".format(shardModified))
      sys.exit(3)
    }
  }

  def isOlderThan(lagMinutes: Int, modified: DateTime): Boolean = {
    modified.plusMinutes(lagMinutes).isBefore(new DateTime(DateTimeZone.UTC))
  }

  def getJsonAt(path: String, client: ZkClient): Option[JValue] = {
    val bytes = client.readData(path).asInstanceOf[Array[Byte]]
    if(bytes != null && bytes.length != 0) {
      Some(JsonParser.parse(new String(bytes)))
    } else {
      None
    }
  }

  def getStatAt(path: String, conn: ZkConnection): Option[Stat] = {
    val zk = conn.getZookeeper
    Option(zk.exists(path, null))
  }
  
  class Config(var limit: Int = 0,
               var lag: Int = 60)
}
