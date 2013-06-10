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
package com.precog.ratatoskr

import com.precog.common._

import com.precog.accounts._
import com.precog.auth._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.jobs._
import com.precog.common.kafka._
import com.precog.common.security._
import com.precog.niflheim._
import com.precog.util._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.vfs._

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo._
import blueeyes.persistence.mongo.dsl._
import blueeyes.util.Clock

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Timeout
import akka.util.Duration
import akka.pattern.{ask, gracefulStop}
import akka.routing.RoundRobinRouter

import _root_.kafka.message._

import au.com.bytecode.opencsv._

import com.weiglewilczek.slf4s.Logging

import org.joda.time._
import org.joda.time.format.ISODateTimeFormat

import org.streum.configrity._

import org.I0Itec.zkclient._

import org.apache.zookeeper.data.Stat

import scopt.mutable.OptionParser

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import scalaz._
import scalaz.effect.IO
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.bifunctor._
import scalaz.syntax.id._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import scala.collection.SortedSet
import scala.collection.SortedMap
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer, ListBuffer}
import scala.io.Source

object Ratatoskr {
  def usage(message: String*): String = {
    val initial = message ++ List("Usage: yggutils {command} {flags/args}",""," For details on a particular command enter yggutils {command} -help", "")

    commands.foldLeft(initial) {
      case (acc, cmd) => acc :+ "%-20s : %s".format(cmd.name, cmd.description)
    }.mkString("\n")
  }

  val commands = List(
    ChownTools,
    KafkaTools,
    IngestTools,
    ZookeeperTools,
    ImportTools,
    CSVTools,
    APIKeyTools
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

object ChownTools extends Command {
  val name = "dbchown"
  val description = "change ownership"

  def run(args: Array[String]) {
    sys.error("FIXME for NIHDB")
  }
}

object KafkaTools extends Command {
  val name = "kafka"
  val description = "Tools for monitoring and viewing kafka queues"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils kafka") {
      opt("r", "range", "<start:stop>", "show message range, e.g.: 5:10 :100 10:", {s: String =>
        val range = MessageRange.parse(s)
        config.range = range.getOrElse(sys.error("Invalid range specification: " + s))
      })
      intOpt("i", "trackInterval", "When running a usage report, stats will be emitted every <interval> messages. Default = 50000", { (i: Int) => config.trackInterval = i })
      opt("l", "local", "dump local kafka file(s)", { config.operation = Some(DumpLocal) })
      opt("c", "central", "dump central kafka file(s)", { config.operation = Some(DumpCentral) })
      opt("u", "unparsed", "dump raw JSON from kafka file(s)", { config.operation = Some(DumpRaw) })
      opt("z", "usageReport", "Run a usage report on the given file(s)", { config.operation = Some(UsageReport) })
      opt("a", "trackArchives", "For the usage report, reset size for a given account when archive messages are encountered", { config.cumulative = false })
      opt("k", "lookupDatabase", "accounts database name", "Use this database for email lookups in usage reports", { (s: String) => config.lookupDatabase = Some(s) })
      opt("v2", "centralToLocal", "convert central kafka file(s) to local kafka format (.local is appended to the new filenames)", { config.operation = Some(ConvertCentral) })
      opt("f", "reportFormat", "The format to use for usage reports. Either csv or json (default)", { (s: String) => s.toLowerCase match {
        case format @ ("csv" | "json") => config.reportFormat = format
        case other => sys.error("Invalid report format: " + other)
      }})
      arglist("<files>", "The files to process", { (s: String) => config.files = config.files :+ (new File(s)) })
    }
    if (parser.parse(args)) {
      process(config)
    } else {
      parser
    }
  }

  def process(config: Config) {
    config.operation.map { _.process(config, config.range) }.getOrElse {
      println("No operation specified!")
    }
  }

  def dump(file: File, range: MessageRange, format: Format) {
    def traverse(itr: Iterator[MessageAndOffset],
                 range: MessageRange,
                 format: Format,
                 i: Int = 0): Unit = {

      if(itr.hasNext && !range.done(i)) {
        val next = itr.next
        if(range.contains(i)) {
          format.dump(i, next)
        }
        traverse(itr, range, format, i+1)
      } else {
        ()
      }
    }

    val ms = new FileMessageSet(file, false)

    traverse(ms.iterator, range, format)
  }

  def convert(central: String): Unit = convert(new File(central), new File(central + ".local"))

  def convert(source: File, destination: File) {
    val src = new FileMessageSet(source, false)
    val dest = new FileMessageSet(destination, true)

    dest.append(src)

    src.close
    dest.close
  }

    sealed trait KafkaAction {
      def process(config: Config, range: MessageRange): Unit
    }

    object ConvertCentral extends KafkaAction {
      def process(config: Config, range: MessageRange) =
        config.files.foreach { f => convert(f, new File(f.getParentFile, f.getName + ".local")) }
    }

    object DumpLocal extends KafkaAction {
      def process(config: Config, range: MessageRange) =
        config.files.foreach { dump(_, range, LocalFormat) }
    }

    object DumpCentral extends KafkaAction {
      def process(config: Config, range: MessageRange) =
        config.files.foreach { dump(_, range, CentralFormat) }
    }

    object DumpRaw extends KafkaAction {
      def process(config: Config, range: MessageRange) =
        config.files.foreach { dump(_, range, RawFormat) }
    }

    object UsageReport extends KafkaAction {
      sealed trait TimeStamp
      case class ExactTime(time: Long, index: Long) extends TimeStamp
      case class Interpolated(index: Long) extends TimeStamp

      // Default to September 25th, 2012 as a lower bound
      var lastTimestamp: ExactTime = ExactTime(1348545600000L, -1L)
      var pendingTimes: List[Interpolated] = Nil

      var interpolationMap: Map[Interpolated, Long] = Map()

      // TODO: This needs to take into account the Authorities as well
      case class ReportState(index: Long, pathSize: Map[Path, Long]) {
        def inc = this.copy(index = index + 1)
      }

      var trackedAccounts: List[AccountId] = Nil
      val slices: ArrayBuffer[(TimeStamp, Map[AccountId,Long])] = ArrayBuffer.empty

      def trackState(state: ReportState): Unit = {
        val byAccount: Map[AccountId, Long] = state.pathSize.groupBy(_._1.elements.head).map {
          case (account, sizes) => (account, sizes.map(_._2).sum)
        }

        import state.index

        val timestamp = lastTimestamp match {
          case ts @ ExactTime(_, `index`) => ts
          case _ => Interpolated(index).unsafeTap { temp =>
            //println("Adding %s to pending timestamps".format(temp))
            pendingTimes ::= temp
          }
        }

        trackedAccounts = trackedAccounts ++ (byAccount.keySet -- trackedAccounts)
        slices += (timestamp -> byAccount)
      }

      def processIngest(trackInterval: Int, state: ReportState, msg: IngestMessage) = {
        // Track timestamps
        (if (msg.timestamp != EventMessage.defaultTimestamp) {
          //println("Exact timestamp found: " + msg.timestamp)
          Some(ExactTime(msg.timestamp.getMillis, state.index))
        } else {
          // see if we can deduce from the data (assuming Nathan's twitter feed or SE postings)
          val timestamps = (msg.data.map (_.value \ "timeStamp") ++ msg.data.map(_.value \ "timestamp")).flatMap {
            case JString(date) =>
              // Dirty hack for trying variations of ISO8601 in use by customers
              List(date, date.replaceFirst(":", "-").replaceFirst(":", "-")).flatMap { date => List(date, date + ".000Z") }
            case _ => None
          }.flatMap { date =>
            try {
              val ts = ISODateTimeFormat.dateTime.parseDateTime(date)
              if (ts.getMillis > lastTimestamp.time) {
                //println("Assigning new timestamp: " + ts)
                Some(ts)
              } else {
                //println("%s is before %s".format(ts, new DateTime(lastTimestamp.time)))
                None
              }
            } catch {
              case t =>
                //println("Error on datetime parse: " + t)
                None
            }
          }

          //println("Deducing timestamp from " + timestamps)

          timestamps.headOption.map { ts => ExactTime(ts.getMillis, state.index) }
        }).foreach { newTimestamp =>
          if (newTimestamp.time <= System.currentTimeMillis) {
            if (pendingTimes.nonEmpty) {
              //println("Updating pending times: " + pendingTimes)
              interpolationMap ++= pendingTimes.map { interp =>
                val interpFraction = (interp.index - lastTimestamp.index).toDouble / (newTimestamp.index - lastTimestamp.index)
                val timeSpanSize = newTimestamp.time - lastTimestamp.time
                val interpTS = (interpFraction * timeSpanSize + lastTimestamp.time).toLong
                (interp, interpTS)
              }

              //println("InterpolationMap now = " + interpolationMap)

              pendingTimes = Nil
            }

            lastTimestamp = newTimestamp
          }
        }

        import msg._
        if (state.index % trackInterval == 0) { trackState(state) }

        if (path.length > 0) {
          val size = data.map(_.value.renderCompact.length).sum

          ReportState(state.index + 1, state.pathSize + (path -> (state.pathSize.getOrElse(path, 0L) + size)))
        } else {
          state.inc
        }
      }

      def process(config: Config, range: MessageRange) = {
        val accountLookup: Map[String, String] = config.lookupDatabase.map { dbName =>
          val mongo = RealMongo(Configuration.parse("servers = [localhost]"))
          val database = mongo.database(dbName)
          implicit val queryTimeout = Timeout(30000)

          Await.result(database(selectAll.from("accounts")).map { results =>
            val built = results.toList
            built.map(_.deserialize[Account]).map { account =>
              (account.accountId, account.email)
            }
          }, queryTimeout.duration).toMap
        }.getOrElse {
          Map.empty
        }

        //println("Got lookup DB:" + accountLookup)

        val finalState = config.files.foldLeft(ReportState(0L, Map.empty)) { case (state, file) =>
          val ms = new FileMessageSet(file, false)

          ms.iterator.grouped(1000).flatMap { _.toSeq.par.map(parseEventMessage) }.foldLeft(state) {
            case (state @ ReportState(index, currentPathSize), parsed) => parsed match {
              case Success(imessage : IngestMessage) =>
                processIngest(config.trackInterval, state, imessage)

              case Success(ArchiveMessage(_, path, _, _, _)) =>
                if (config.cumulative) {
                  state.inc
                } else {
                  trackState(state)

                  if (path.length > 0) {
                    //println("Deleting from path " + path)
                    ReportState(index + 1, currentPathSize + (path -> 0L)).unsafeTap { newState =>
                      trackState(newState)
                    }
                  } else {
                    state.inc
                  }
                }

              case other =>
                println("## Skipping undesired data: " + other)
                //RawFormat.dump(0, msg)
                state.inc
            }
          }
        }

        trackState(finalState)

        if (config.reportFormat == "csv") {
          println("index,total," + trackedAccounts.sorted.map { acct => "\"%s\"".format(accountLookup.getOrElse(acct, acct)) }.mkString(","))
          slices.foreach { case (index, byAccount) =>
              val accountTotals = trackedAccounts.sorted.map(byAccount.getOrElse(_, 0L))
              (index match {
                case ExactTime(time, _) => Some(time)
                case i: Interpolated => interpolationMap.get(i)
              }).foreach { timestamp =>
                //println(index + " => " + timestamp)
                println("%d,%d,%s".format(timestamp, accountTotals.sum, accountTotals.mkString(",")))
              }
          }
        } else {
          slices.foreach { case (index, byAccount) =>
              (index match {
                case ExactTime(time, _) => Some(time)
                case i: Interpolated => interpolationMap.get(i)
              }).foreach { timestamp =>
                //println(index + " => " + timestamp)
                println(JObject("index" -> JNum(timestamp), "account" -> JString("total"), "size" -> JNum(byAccount.map(_._2).sum)).renderCompact)
                trackedAccounts.foreach { account =>
                  if (byAccount.contains(account)) {
                    println(JObject("index" -> JNum(timestamp), "account" -> JString(accountLookup.getOrElse(account, account)), "size" -> JNum(byAccount.getOrElse(account, 0L))).renderCompact)
                  }
                }
              }
          }
        }
        sys.exit(0) // due to BlueEyes holding things open for mongo
      }
    }

  class Config(var operation: Option[KafkaAction] = None,
               var files: List[File] = Nil,
               var trackInterval: Int = 50000,
               var cumulative: Boolean = true,
               var lookupDatabase: Option[String] = None,
               var reportFormat: String = "json",
               var range: MessageRange = MessageRange.ALL)

  case class MessageRange(start: Option[Int], finish: Option[Int]) {

    def done(i: Int): Boolean = finish.map { i >= _ }.getOrElse(false)

    def contains(i: Int): Boolean =
      (start.map{_ <= i}, finish.map{i < _}) match {
        case (Some(s), Some(f)) => s && f
        case (Some(s), None   ) => s
        case (None   , Some(f)) => f
        case (None   , None   ) => true
      }
  }

  object MessageRange {
    val ALL = MessageRange(None,None)

    def parse(s: String): Option[MessageRange] = {
      val parts = s.split(":")
      if(parts.length == 2) {
        (parseOffset(parts(0)), parseOffset(parts(1))) match {
          case (Right(s), Right(f)) => Some(MessageRange(s,f))
          case _                    => None
        }
      } else {
        None
      }
    }

    def parseOffset(s: String): Either[Unit, Option[Int]] = {
       try {
         Right(if(s.trim.length == 0) {
           None
         } else {
           Some(s.toInt)
         })
       } catch {
         case ex => Left("Parse error for: " + s)
       }
    }
  }

  def parseEventMessage(msg: MessageAndOffset) = EventMessageEncoding.read(msg.message.payload).flatMap {
    case \/-(parsed : IngestMessage) =>
      Success(parsed)

    case -\/((_, path, msgGen)) =>
      path.elements.headOption.map { account =>
        msgGen(Authorities(account)).asInstanceOf[IngestMessage]
      }.toSuccess("Could not determine account from path")

    case \/-(parsed: ArchiveMessage) =>
      Success(parsed)

    case other =>
      Failure("Could not determine message from " + other)
  }

  sealed trait Format {
    def dump(i: Int, msg: MessageAndOffset)
  }

  case object RawFormat extends Format {
    def dump(i: Int, msg: MessageAndOffset) {
      val message = msg.message.payload
      // Read past magic, type, and stop bytes
      message.get()
      val tpe = message.get()
      message.get()

      val bytes = new Array[Byte](message.remaining)

      message.get(bytes)

      println("Type: %d, offset: %d, payload: %s".format(tpe, msg.offset, new String(bytes, "UTF-8")))
    }
  }

  case object LocalFormat extends Format {
    def dump(i: Int, msg: MessageAndOffset) {
      EventEncoding.read(msg.message.payload) match {
        case Success(Ingest(apiKey, path, ownerAccountId, data, _, _, _)) =>
          println("Ingest-%06d Offset: %d Path: %s APIKey: %s Owner: %s --".format(i+1, msg.offset, path, apiKey, ownerAccountId))
          data.foreach(v => println(v.renderPretty))

        case other =>
          println("Message %d: %s was not an ingest request.".format(i+1, other.toString))
      }
    }
  }

  case object CentralFormat extends Format {
    def dump(i: Int, msg: MessageAndOffset) {
      val parsed = parseEventMessage(msg)

      parsed.foreach {
        case IngestMessage(apiKey, path, ownerAccountId, data, _, _, _) =>
          println("IngestMessage-%06d Offset: %d, Path: %s APIKey: %s Owner: %s".format(i+1, msg.offset, path, apiKey, ownerAccountId))
          data.foreach(v => println(v.serialize.renderPretty))
      }

      parsed.valueOr {
        case other =>
          println("Message %d: %s was not an ingest request.".format(i+1, other.toString))
      }
    }
  }
}

object ZookeeperTools extends Command {

  val name = "zk"
  val description = "Misc zookeeper tools"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils zk") {
      opt("z", "zookeeper", "The zookeeper host:port", { s: String => config.zkConn = s })
      opt("c", "checkpoints", "Show shard checkpoint state with prefix", { s: String => config.showCheckpoints = Some(s)})
      opt("a", "agents", "Show ingest agent state with prefix", { s: String => config.showAgents = Some(s)})
      opt("uc", "update_checkpoints", "Update agent state. Format = path:json", {s: String => config.updateCheckpoint = Some(s)})
      opt("ua", "update_agents", "Update agent state. Format = path:json", {s: String => config.updateAgent = Some(s)})
    }
    if (parser.parse(args)) {
      val conn: ZkConnection = new ZkConnection(config.zkConn)
      val client: ZkClient = new ZkClient(conn)
      try {
        process(conn, client, config)
      } finally {
        client.close
      }
    } else {
      parser
    }
  }

  def process(conn: ZkConnection, client: ZkClient, config: Config) {
    config.showCheckpoints.foreach { path =>
      showChildren("checkpoints", path, pathsAt(path, client))
    }
    config.showAgents.foreach { path =>
      showChildren("agents", path, pathsAt(path, client))
    }

    def parseCheckpoint(data: String) = ((Extractor.Thrown(_:Throwable)) <-: JParser.parseFromString(data)).flatMap(_.validated[YggCheckpoint])

    def setCheckpoint(path: String, data: YggCheckpoint) {
      if (!client.exists(path)) client.createPersistent(path, true)

      val updater = new DataUpdater[Array[Byte]] {
        def update(cur: Array[Byte]): Array[Byte] = data.serialize.renderCompact.getBytes
      }

      client.updateDataSerialized(path, updater)
      println("Checkpoint updated: %s with %s".format(path, data))
    }

    config.checkpointUpdate.foreach {
      case (path, data) =>
        data match {
          case "initial" =>
            println("Loading initial checkpoint")
            setCheckpoint(path, YggCheckpoint.Empty)
          case s =>
            println("Loading initial checkpoint from : " + s)
            setCheckpoint(path, parseCheckpoint(s).valueOr(err => sys.error(err.message)))
        }
    }

    config.relayAgentUpdate.foreach {
      case (path, data) =>
        setCheckpoint(path, parseCheckpoint(data).valueOr(err => sys.error(err.message)))
    }
  }

  def showChildren(name: String, path: String, children: Buffer[(String, String)]) { children match {
      case l if l.size == 0 =>
        println("no %s at: %s".format(name, path))
      case children =>
        println("%s for: %s".format(name, path))
        children.foreach {
          case (child, data) =>
            println("  %s".format(child))
            println("    %s".format(data))
        }
    }
  }

  def pathsAt(path: String, client: ZkClient): Buffer[(String, String)] = {
    val children = client.watchForChilds(path)
    if(children == null) {
      ListBuffer[(String, String)]()
    } else {
      children.asScala map { child =>
        val bytes = client.readData(path + "/" + child).asInstanceOf[Array[Byte]]
        (child, new String(bytes))
      }
    }
  }

  class Config(var zkConn: String = "localhost:2181",
               var showCheckpoints: Option[String] = None,
               var showAgents: Option[String] = None,
               var updateCheckpoint: Option[String] = None,
               var updateAgent: Option[String] = None) {

    def splitPathJson(s: String): (String,String) = s.split(":", 2) match {
      case Array(path,json) => (path, json)
      case _ => sys.error("Invalid format for path+json: \"%s\"".format(s))
    }

    def checkpointUpdate() = updateCheckpoint.map(splitPathJson)

    def relayAgentUpdate() = updateAgent.map(splitPathJson)
  }
}

object IngestTools extends Command {
  val name = "ingest"
  val description = "Show details about ingest status"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils ingest_status") {
      intOpt("s", "limit", "<sync-limit-messages>", "if sync is greater than the specified limit an error will occur", {s: Int => config.limit = s})
      intOpt("l", "lag", "<time-lag-minutes>", "if update lag is greater than the specified value an error will occur", {l: Int => config.lag = l})
      opt("z", "zookeeper", "The zookeeper host:port", { s: String => config.zkConn = s })
      opt("c", "shardpath", "The shard's ZK path", { s: String => config.shardZkPath = s })
      opt("r", "relaypath", "The relay's ZK path", { s: String => config.relayZkPath = s })
    }
    if (parser.parse(args)) {
      val conn = new ZkConnection(config.zkConn)
      val client = new ZkClient(conn)
      try {
        process(conn, client, config)
      } finally {
        client.close
      }
    } else {
      parser
    }
  }

  val shardCheckpointPath = "/beta/com/precog/ingest/v1/shard/checkpoint/shard01"
  val relayAgentPath = "/test/com/precog/ingest/v1/relay_agent/qclus-demo01"

  def process(conn: ZkConnection, client: ZkClient, config: Config) {
    val relayRaw = getJsonAt(config.relayZkPath, client).getOrElse(sys.error("Error reading relay agent state"))
    val shardRaw = getJsonAt(config.shardZkPath, client).getOrElse(sys.error("Error reading shard state"))

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
    if (bytes == null || bytes.length == 0)
      None
    else
      JParser.parseFromByteBuffer(ByteBuffer.wrap(bytes)).toOption
  }

  def getStatAt(path: String, conn: ZkConnection): Option[Stat] = {
    val zk = conn.getZookeeper
    Option(zk.exists(path, null))
  }

  class Config(var limit: Int = 0,
               var lag: Int = 60,
               var zkConn: String = "localhost:2181",
               var shardZkPath: String = "/",
               var relayZkPath: String = "/")
}

object ImportTools extends Command with Logging {
  val name = "import"
  val description = "Bulk import of json/csv data directly to data columns"

  val sid = new AtomicInteger(0)

  class Config(
    var input: Vector[(String, String)] = Vector.empty,
    val batchSize: Int = 10000,
    var apiKey: APIKey = "not-a-real-key",
    var accountId: AccountId = "not-a-real-account",
    var verbose: Boolean = false ,
    var storageRoot: File = new File("./data"),
    var archiveRoot: File = new File("./archive")
  )

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils import") {
      opt("t", "token", "<api key>", "authorizing API key", { s: String => config.apiKey = s })
      opt("o", "owner", "<account id>", "Owner account ID to insert data under", { s: String => config.accountId = s })
      opt("s", "storage", "<storage root>", "directory containing data files", { s: String => config.storageRoot = new File(s) })
      opt("a", "archive", "<archive root>", "directory containing archived data files", { s: String => config.archiveRoot = new File(s) })
      arglist("<json input> ...", "json input file mappings {db}={input}", {s: String =>
        val parts = s.split("=")
        val t = (parts(0) -> parts(1))
        config.input = config.input :+ t
      })
    }

    if (parser.parse(args)) {
      process(config)
    } else {
      parser
    }
  }

  def process(config: Config) {
    println("Storage root = " + config.storageRoot)
    config.storageRoot.mkdirs
    config.archiveRoot.mkdirs

    val stopTimeout = Duration(310, "seconds")
    val authorities = Authorities(config.accountId)

    implicit val actorSystem = ActorSystem("yggutilImport")
    implicit val defaultAsyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
    implicit val M = new FutureMonad(ExecutionContext.defaultExecutionContext(actorSystem))

    class YggConfig(val config: Configuration) extends BaseConfig {
      val cookThreshold = config[Int]("precog.jdbm.maxSliceSize", 20000)
      val clock = blueeyes.util.Clock.System
      val storageTimeout = Timeout(Duration(120, "seconds"))
      val quiescenceTimeout = Duration(300, "seconds")
    }

    val yggConfig = new YggConfig(Configuration.parse("precog.storage.root = " + config.storageRoot))

    val chefs = (1 to 8).map { _ =>
      actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))
    }
    val masterChef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))

    val accountFinder = new StaticAccountFinder[Future](config.accountId, config.apiKey, Some("/"))

    logger.info("Starting APIKeyFinder")
    //// ****** WARNING ****** ////
    // IF YOU EVER CHANGE THE FOLLOWING LINE TO USE ANYTHING BUT THE IN-MEMORY SYSTEM,
    // YOU MUST FIX grantWrite OR YOU'LL GIVE ROOT PERMISSIONS TO THE API KEY BEING
    // USED FOR THE WRITE
    val apiKeyManager = new InMemoryAPIKeyManager[Future](Clock.System)
    //// ****** END WARNING ****** ////

    val apiKeyFinder = new DirectAPIKeyFinder(apiKeyManager)

    // This uses an empty checkpoint because there is no support for insertion/metadata
    object vfsModule extends ActorVFSModule { self =>
      logger.info("Starting ResourceBuilder")
      val jobManager = new InMemoryJobManager[Future]
      val permissionsFinder = new PermissionsFinder(apiKeyFinder, accountFinder, new Instant(0L))
      val resourceBuilder = new ResourceBuilder(actorSystem, yggConfig.clock, masterChef, yggConfig.cookThreshold, yggConfig.storageTimeout)

      logger.info("Starting Projections Actor")
      val projectionsActor = actorSystem.actorOf(Props(new PathRoutingActor(yggConfig.dataDir, yggConfig.storageTimeout.duration, yggConfig.quiescenceTimeout, 100, yggConfig.clock)))

      logger.info("Shard module complete")
    }

    import AsyncParser._

    val pid: Int = System.currentTimeMillis.toInt & 0x7fffffff
    logger.info("Using PID: " + pid)
    implicit val insertTimeout = Timeout(300 * 1000)

    def grantWrite(key: APIKey) = 
      for { 
        rootKey <- apiKeyManager.rootAPIKey 
        rootGrantId <- apiKeyManager.rootGrantId 
        _ <- apiKeyManager.populateAPIKey(None, None, rootKey, key, Set(rootGrantId)) onComplete {
          case Left(error) => logger.error("Could not add grant " + rootGrantId + " to apiKey " + key, error)
          case Right(success) => logger.info("Updated API key record: " + success)
        }
      } yield key

    def logGrants(key: APIKey) = apiKeyManager.validGrants(key, None) onComplete {
      case Left(error) => logger.error("Could not retrieve grants for api key " + key, error)
      case Right(success) => logger.info("Grants for " + key + ": " + success)
    }

    def runIngest(apiKey: APIKey) = config.input.toStream traverse {
      case (db, input) =>
        val path = Path(db)
        logger.info("Inserting batch: %s:%s".format(db, input))

        val bufSize = 8 * 1024 * 1024
        val f = new File(input)
        val ch = new FileInputStream(f).getChannel
        val bb = ByteBuffer.allocate(bufSize)

        def loop(offset: Long, p: AsyncParser): Future[Unit] = {
          val n = ch.read(bb)
          bb.flip()

          val input = if (n >= 0) More(bb) else Done
          val (AsyncParse(errors, results), parser) = p(input)

          if (!errors.isEmpty) {
            sys.error("found %d parse errors.\nfirst 5 were: %s" format (errors.length, errors.take(5)))
          } else if (results.size > 0) {
            val eventidobj = EventId(pid, sid.getAndIncrement)
            logger.info("Sending %d events".format(results.size))
            val records = results map { IngestRecord(eventidobj, _) }
            val update = IngestData(
              Seq((offset, IngestMessage(apiKey, path, authorities, records, None, yggConfig.clock.instant, StreamRef.Append)))
            )

            (vfsModule.projectionsActor ? update) flatMap { _ =>
              logger.info("Batch saved")
              bb.flip()
              if (n >= 0) loop(offset + 1, parser) else Future(())
            }
          } else {
            if (n >= 0) loop(offset + 1, parser) else Future(())
          }
        }
        
        loop(0L, AsyncParser.stream()) onComplete { 
          case _ => ch.close()
        }
    }

    val complete = 
      grantWrite(config.apiKey) >>
      logGrants(config.apiKey) >> 
      runIngest(config.apiKey) >>
      Future(logger.info("Finalizing chef work-in-progress")) >>
      chefs.toList.traverse(gracefulStop(_, stopTimeout)) >>
      gracefulStop(masterChef, stopTimeout) >>
      Future(logger.info("Completed chef shutdown")) >>
      Future(logger.info("Waiting for shard shutdown")) >>
      gracefulStop(vfsModule.projectionsActor, stopTimeout)

    Await.result(complete, stopTimeout)
    actorSystem.shutdown()

    logger.info("Shutdown")
    sys.exit(0)
  }
}

object CSVTools extends Command {
  val name = "csv"
  val description = "Convert CSV file to JSON"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils csv") {
      opt("d","delimeter","field delimeter", {s: String =>
        if(s.length == 1) {
          config.delimeter = s.charAt(0)
        } else {
          sys.error("Invalid delimeter")
        }
      })
      opt("t","mapTimestamps","Map timestamps to expected format.", { config.teaseTimestamps = true })
      opt("v","verbose","Map timestamps to expected format.", { config.verbose = true })
      arg("<csv_file>", "csv file to convert (headers required)", {s: String => config.input = s})
    }
    if (parser.parse(args)) {
      process(config)
    } else {
      parser
    }
  }

  def process(config: Config) {
    CSVToJSONConverter.convert(config.input, config.delimeter, config.teaseTimestamps, config.verbose).foreach {
      case jval => println(jval.renderCompact)
    }
  }

  class Config(
    var input: String = "",
    var delimeter: Char = ',',
    var teaseTimestamps: Boolean = false,
    var verbose: Boolean = false)
}


object APIKeyTools extends Command with AkkaDefaults with Logging {
  val name = "tokens"
  val description = "APIKey management utils"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils csv") {
      opt("l","list","List API keys", { config.list = true })
//      opt("c","children","List children of API key", { s: String => config.listChildren = Some(s) })
      opt("n","new","New customer account at path", { s: String => config.accountId = Some(s) })
      opt("r","root","Show root API key", { config.showRoot = true })
      opt("c","create","Create root API key", { config.createRoot = true; config.showRoot = true })
      opt("a","name","Human-readable name for new API key", { s: String => config.newAPIKeyName = s })
      opt("x","delete","Delete API key", { s: String => config.delete = Some(s) })
      opt("d","database","APIKey database name (ie: beta_auth_v1)", {s: String => config.database = s })
      opt("t","tokens","APIKeys collection name", {s: String => config.collection = s })
      opt("a","archive","Collection for deleted API keys", {s: String => config.deleted = Some(s) })
      opt("s","servers","Mongo server config", {s: String => config.servers = s})
    }
    if (parser.parse(args)) {
      process(config)
    } else {
      parser
    }
  }

  def process(config: Config) {
    val job = for {
      (apiKeys, stoppable) <- apiKeyManager(config)
      actions = (config.list).option(list(apiKeys)).toSeq ++
                (config.showRoot).option(showRoot(apiKeys)).toSeq ++
//              config.listChildren.map(listChildren(_, apiKeys)) ++
                config.accountId.map(p => create(p, config.newAPIKeyName, apiKeys)) ++
                config.delete.map(delete(_, apiKeys))
      _ <- Future.sequence(actions)
      _ <- Stoppable.stop(stoppable)
    } yield {
      defaultActorSystem.shutdown
    }

    Await.result(job, Duration(30, "seconds"))
  }

  def apiKeyManager(config: Config): Future[(APIKeyManager[Future], Stoppable)] = {
    val mongo = RealMongo(config.mongoConfig)
    implicit val timeout = config.mongoSettings.timeout
    val database = mongo.database(config.database)

    val dbStop = Stoppable.fromFuture(database.disconnect.fallbackTo(Future(())) flatMap { _ => mongo.close })

    val rootKey: Future[APIKeyRecord] = if (config.createRoot) {
      MongoAPIKeyManager.createRootAPIKey(
        database,
        config.mongoSettings.apiKeys,
        config.mongoSettings.grants
      )
    } else {
      MongoAPIKeyManager.findRootAPIKey(
        database,
        config.mongoSettings.apiKeys
      )
    }

    rootKey map { k => (new MongoAPIKeyManager(mongo, database, config.mongoSettings.copy(rootKeyId = k.apiKey)), dbStop) }
  }

  def list(apiKeyManager: APIKeyManager[Future]) = {
    for (apiKeys <- apiKeyManager.listAPIKeys) yield {
      apiKeys.foreach(printAPIKey)
    }
  }

  def showRoot(apiKeyManager: APIKeyManager[Future]) = {
    for (rootAPIKey <- apiKeyManager.rootAPIKey) yield {
      println(rootAPIKey)
    }
  }

  def printAPIKey(t: APIKeyRecord): Unit = {
    println(t)
//    println("APIKey: %s Issuer: %s".format(t.uid, t.issuer.getOrElse("NA")))
//    println("  Permissions (Path)")
//    t.permissions.path.foreach { p =>
//      println("    " + p)
//    }
//    println("  Permissions (Data)")
//    t.permissions.data.foreach { p =>
//      println("    " + p)
//    }
//    println("  Grants")
//    t.grants.foreach { g =>
//      println("    " + g)
//    }
//    println()
//  }

//  def listChildren(apiKey: String, apiKeyManager: APIKeyManager[Future]) = {
//    for (Some(parent) <- apiKeyManager.findAPIKey(apiKey); children <- apiKeyManager.listChildren(parent)) yield {
//      children.foreach(printAPIKey)
//    }
//  }
  }

  def create(accountId: String, apiKeyName: String, apiKeyManager: APIKeyManager[Future]) = {
    apiKeyManager.newStandardAPIKeyRecord(accountId, Some(apiKeyName))
  }

  def delete(t: String, apiKeyManager: APIKeyManager[Future]) = sys.error("todo")
//    for (Some(apiKey) <- apiKeyManager.findAPIKey(t); t <- apiKeyManager.deleteAPIKey(apiKey)) yield {
//      println("Deleted API key: ")
//      printAPIKey(apiKey)
//    }
//  }

  class Config {
    var delete: Option[String] = None
    var accountId: Option[String] = None
    var newAPIKeyName: String = ""
    var showRoot: Boolean = false
    var createRoot: Boolean = false
    var list: Boolean = false
    var listChildren: Option[String] = None
    var database: String = "auth_v1"
    var collection: String = "tokens"
    var deleted: Option[String] = None
    var servers: String = "localhost"

    def deletedCollection: String = {
      deleted.getOrElse( collection + "_deleted" )
    }

    def mongoSettings: MongoAPIKeyManagerSettings = MongoAPIKeyManagerSettings(
      apiKeys = collection,
      deletedAPIKeys = deletedCollection
    )

    def mongoConfig: Configuration = {
      Configuration.parse("servers = %s".format(mongoServers))
    }

    def mongoServers: String = {
      val s = servers.trim
      if (s.startsWith("[") && s.endsWith("]")) s else "[" + s + "]"
    }
  }
}

object CSVToJSONConverter {
  def convert(file: String, delimeter: Char = ',', timestampConversion: Boolean = false, verbose: Boolean = false): Iterator[JValue] = new Iterator[JValue] {

    private val reader = new CSVReader(new FileReader(file), delimeter)
    private val header = reader.readNext
    private var line = reader.readNext

    def hasNext(): Boolean = line != null

    def next(): JValue = {
      val result = JObject(header.zip(line).map{
        case (k, v) => JField(k, parse(v, timestampConversion, verbose))
      }.toList)
      line = reader.readNext
      result
    }
  }

  private val Timestamp = """^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3})\d{0,3}$""".r

  def parse(s: String, ts: Boolean = false, verbose: Boolean = false): JValue = {
    JParser.parseFromString(s) getOrElse {
      s match {
        case Timestamp(d, t) if (ts) => JString("%sT%sZ".format(d,t))
        case s => JString(s)
      }
    }
  }
}
