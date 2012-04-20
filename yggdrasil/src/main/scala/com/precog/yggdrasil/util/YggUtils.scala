package com.precog.yggdrasil
package util

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Timeout
import akka.util.Duration

import org.joda.time._

import com.precog.common._
import com.precog.common.util._
import com.precog.common.kafka._
import com.precog.common.security._
import com.precog.yggdrasil.leveldb._
import com.precog.yggdrasil.actor._

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._

import java.io.File
import java.nio.ByteBuffer

import collection.mutable.{Buffer, ListBuffer}
import collection.JavaConversions._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._
import blueeyes.bkka.AkkaDefaults
import blueeyes.persistence.mongo._

import scopt.mutable.OptionParser

import scala.collection.SortedSet
import scala.collection.SortedMap

import _root_.kafka.message._

import org.I0Itec.zkclient._
import org.apache.zookeeper.data.Stat

import scalaz.{Success, Failure}

import org.streum.configrity._

import java.util.concurrent.atomic.AtomicInteger
import scala.io.Source

import au.com.bytecode.opencsv._
import java.io._

object YggUtils {
 
  def usage(message: String*): String = {
    val initial = message ++ List("Usage: yggutils {command} {flags/args}",""," For details on a particular command enter yggutils {command} -help", "")
    
    commands.foldLeft(initial) {
      case (acc, cmd) => acc :+ "%-20s : %s".format(cmd.name, cmd.description)
    }.mkString("\n")

  }

  val commands = List(
    DatabaseTools,
    KafkaTools,
    IngestTools,
    ZookeeperTools,
    ImportTools,
    CSVTools,
    TokenTools
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

object DatabaseTools extends Command {
  val name = "db" 
  val description = "describe db paths and selectors" 
  
  def run(args: Array[String]) {
    println(args.mkString(","))
    val config = new Config
    val parser = new OptionParser("yggutils db") {
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

object KafkaTools extends Command {
  val name = "kafka"
  val description = "Tools for monitoring and viewing kafka queues"

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils kafka") {
      opt("r", "range", "<start:stop>", "show message range ie: 5:10 :100 10:", {s: String => 
        val range = MessageRange.parse(s)
        config.range = range.getOrElse(sys.error("Invalid range specification: " + s))
      })
      opt("l", "local", "<local kafka file>", "local kafka file", {s: String => config.dumpLocal = Some(s) })
      opt("c", "central", "<central kafka file>", "dump central kafka file", {s: String => config.dumpCentral = Some(s) })
      opt("c2l", "centralToLocal", "<central kafka file>", "convert central kafka file to local kafak", {s: String => config.convertCentral = Some(s) })
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }

  def process(config: Config) {
    import config._
    dumpLocal.foreach { f => dump(new File(f), range, LocalFormat) } 
    dumpCentral.foreach { f => dump(new File(f), range, CentralFormat) } 
    convertCentral.foreach { convert(_) }
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
    val eventCodec = LocalFormat.codec 
    val ingestCodec = CentralFormat.codec 
    
    val src = new FileMessageSet(source, false) 
    val dest = new FileMessageSet(destination, true) 

    val outMessages = src.iterator.toList.map { mao =>
      ingestCodec.toEvent(mao.message) match {
        case EventMessage(_, event) =>
          eventCodec.toMessage(event)  
        case _ => sys.error("Unknown message type")
      }
    }

    val outSet = new ByteBufferMessageSet(messages = outMessages.toArray: _*)
    dest.append(outSet)

    src.close
    dest.close
  }

  class Config(var convertCentral: Option[String] = None, 
               var dumpLocal: Option[String] = None,
               var dumpCentral: Option[String] = None,
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

  sealed trait Format {
    def dump(i: Int, msg: MessageAndOffset)
  }

  case object LocalFormat extends Format {
    val codec = new KafkaEventCodec

    def dump(i: Int, msg: MessageAndOffset) {
      val event = codec.toEvent(msg.message)
      println("Event-%06d Path: %s Token: %s".format(i+1, event.path, event.tokenId))
      println(Printer.pretty(Printer.render(event.data)))
    }
  }

  case object CentralFormat extends Format {
    val codec = new KafkaIngestMessageCodec

    def dump(i: Int, msg: MessageAndOffset) {
      codec.toEvent(msg.message) match {
        case EventMessage(EventId(pid, sid), Event(path, tokenId, data, _)) =>
          println("Event-%06d Id: (%d/%d) Path: %s Token: %s".format(i+1, pid, sid, path, tokenId))
          println(Printer.pretty(Printer.render(data)))
        case _ =>
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
      opt("uc", "update_checkpoints", "Update agent state prefix:shard:json", {s: String => config.updateCheckpoint = Some(s)})
      opt("ua", "update_agents", "Update agent state prefix:ingest:json", {s: String => config.updateAgent = Some(s)})
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

  val shardCheckpointPath = "/com/precog/ingest/v1/shard/checkpoint"
  val ingestAgentPath = "/com/precog/ingest/v1/relay_agent"

  def process(conn: ZkConnection, client: ZkClient, config: Config) {
    config.checkpointsPath().foreach { path =>
      showChildren("checkpoints", path, pathsAt(path, client))
    }
    config.relayAgentsPath().foreach { path =>
      showChildren("agents", path, pathsAt(path, client))
    }
    config.checkpointUpdate.foreach {
      case (path, data) =>
        JsonParser.parse(data).validated[YggCheckpoint] match {
          case Success(_) =>
            client.updateDataSerialized(path, new DataUpdater[Array[Byte]] {
              def update(cur: Array[Byte]): Array[Byte] = data.getBytes 
            })  
            println("Checkpoint updated: %s with %s".format(path, data))
          case Failure(e) => println("Invalid json for checkpoint: %s".format(e))
      }
    }
    config.relayAgentUpdate.foreach { 
      case (path, data) =>
        JsonParser.parse(data).validated[EventRelayState] match {
          case Success(_) =>
            client.updateDataSerialized(path, new DataUpdater[Array[Byte]] {
              def update(cur: Array[Byte]): Array[Byte] = data.getBytes 
            })  
            println("Agent updated: %s with %s".format(path, data))
          case Failure(e) => println("Invalid json for agent: %s".format(e))
      }
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
      children.map{ child =>
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

    def checkpointsPath() = showCheckpoints.map { buildPath(_, shardCheckpointPath) } 
    def relayAgentsPath() = showAgents.map { buildPath(_, ingestAgentPath) } 
    
    def buildPath(prefix: String, base: String): String = 
      if(prefix.trim.size == 0) {
        base 
      } else {
        "/%s%s".format(prefix, base) 
      }

    def splitOn(delim: Char, s: String): (String, String) = {
      val t = s.span( _ != delim)
      (t._1, t._2.substring(1))
    }

    def splitUpdate(s: String): (String, String, String) = {
      val t1 = splitOn(':', s)
      val t2 = splitOn(':', t1._2)
      (t1._1, t2._1, t2._2)
    }

    def checkpointUpdate() = updateCheckpoint.map{ splitUpdate }.map {
      case (pre, id, data) =>
        val path = buildPath(pre, shardCheckpointPath) + "/" + id
        (path, data)
    }

    def relayAgentUpdate() = updateAgent.map{ splitUpdate }.map {
      case (pre, id, data) =>
        val path = buildPath(pre, ingestAgentPath) + "/" + id
        (path, data)
    }
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
               var lag: Int = 60,
               var zkConn: String = "localhost:2181")
}

object ImportTools extends Command {
  val name = "import"
  val description = "Bulk import of json/csv data directly to data columns"
  

  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils import") {
      booleanOpt("v", "verbose", "verbose logging", {b: Boolean => config.verbose = b})
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
    val dir = new File("./data") 
    dir.mkdirs

    object shard extends ActorYggShard[IterableDataset] with StandaloneActorEcosystem {
      class YggConfig(val config: Configuration) extends BaseConfig with ProductionActorConfig {

      }
      val yggConfig = new YggConfig(Configuration.parse("precog.storage.root = " + dir.getName))
      val yggState = YggState(dir, Map.empty, Map.empty)
    }
    Await.result(shard.actorsStart, Duration(60, "seconds"))
    config.input.foreach {
      case (db, input) =>
        if(config.verbose) println("Inserting batch: %s:%s".format(db, input))
        val events = Source.fromFile(input).getLines
        insert(config, db, events, shard)
    }
    if(config.verbose) println("Waiting for shard shutdown")
    Await.result(shard.actorsStop, Duration(60, "seconds"))
    if(config.verbose) println("Shutdown")
  }

  val sid = new AtomicInteger(0)

  def insert(config: Config, db: String, itr: Iterator[String], shard: YggShard[IterableDataset], batch: Vector[EventMessage] = Vector.empty, b: Int = 0): Unit = {
    val verbose = config.verbose
    val batchSize = config.batchSize
    val token = config.token
    val (curBatch, nb) = if(batch.size >= batchSize || !itr.hasNext) {
      if(verbose) println("Saving batch - " + b)
      Await.result(shard.storeBatch(batch, new Timeout(60000)), Duration(60, "seconds"))
      (Vector.empty[EventMessage], b+1)
    } else {
      (batch, b)
    }
    if(itr.hasNext) {
      val data = JsonParser.parse(itr.next) 
      val event = Event(Path(db), token, data, Map.empty)
      val em = EventMessage(EventId(0, sid.getAndIncrement), event)
      insert(config, db, itr, shard, curBatch :+ em, nb)
    } else {
      ()
    }
  }

  class Config(
    var input: Vector[(String, String)] = Vector.empty, 
    val batchSize: Int = 1000, 
    val token: String = TestTokenManager.rootUID,
    var verbose: Boolean = false 
  )
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
      booleanOpt("t","mapTimestamps","Map timestamps to expected format.", {b: Boolean => 
        config.teaseTimestamps = b
      })
      arg("<csv_file>", "csv file to convert (headers required)", {s: String => config.input = s}) 
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }

  def process(config: Config) {
    CSVToJSONConverter.convert(config.input, config.delimeter, config.teaseTimestamps).foreach {
      case jval => println(Printer.compact(Printer.render(jval)))
    }
  }

  class Config(
    var input: String = "", 
    var delimeter: Char = ',', 
    var teaseTimestamps: Boolean = false)
}

object TokenTools extends Command with AkkaDefaults {
  val name = "tokens"
  val description = "Token management utils"
    
  def run(args: Array[String]) {
    val config = new Config
    val parser = new OptionParser("yggutils csv") {
      booleanOpt("l","list","List tokens", { b: Boolean => config.list = b })
      opt("n","new","New customer account at path", { s: String => config.newAccount = Some(s) })
      opt("x","delete","Delete token", { s: String => config.delete = Some(s) })
      opt("d","database","Token database name (ie: beta_auth_v1)", {s: String => config.database = s })
      opt("t","tokens","Tokens collection name", {s: String => config.collection = s }) 
      opt("r","root","root token for creation", {s: String => config.root = s })
      opt("a","archive","Collection for deleted tokens", {s: String => config.deleted = Some(s) })
      opt("s","servers","Mongo server config", {s: String => config.servers = s})
    }
    if (parser.parse(args)) {
      process(config)
    } else { 
      parser
    }
  }
  
  def process(config: Config) {
    try {
      val tm = tokenManager(config)
      try {
        if(config.list) list(config, tm) 
        config.newAccount.foreach { create(_, config, tm) }
        config.delete.foreach { delete(_, config, tm) }
      } finally {
        Await.result(tm.close(), Duration(10, "seconds"))
      }
    } finally {
      defaultActorSystem.shutdown
    }
  }

  def tokenManager(config: Config): TokenManager = {
    val mongo = RealMongo(config.mongoConfig)
    new MongoTokenManager(mongo, mongo.database(config.database), config.collection, config.deletedCollection, new Timeout(10000))
  }

  def list(config: Config, tokenManager: TokenManager) {
    val f = tokenManager.list
    Await.result(f, Duration(10, "seconds")).foreach { t =>
      println("Token: %s Issuer: %s".format(t.uid, t.issuer.getOrElse("NA")))
      println("  Permissions (Path)")
      t.permissions.path.foreach { p =>
        println("    " + p)
      }
      println("  Permissions (Data)")
      t.permissions.data.foreach { p =>
        println("    " + p)
      }
      println("  Grants")
      t.grants.foreach { g =>
        println("    " + g)
      }
      println()
    }
  }

  def create(p: String, config: Config, tokenManager: TokenManager) {
    val perms = TestTokenManager.standardAccountPerms(p)
    val f = tokenManager.issueNew(Some(config.root), perms, Set.empty, false)
    Await.result(f, Duration(10, "seconds"))
  }

  def delete(t: String, config: Config, tokenManager: TokenManager) {
    val f = tokenManager.lookup(t).flatMap { _.map { tokenManager.deleteToken(_).map{ _ => () } }.getOrElse{Future(())}}
    Await.result(f, Duration(10, "seconds"))
  }
  
  class Config(
    var delete: Option[String] = None, 
    var newAccount: Option[String] = None,
    var root: String = TestTokenManager.rootUID,
    var list: Boolean = false,
    var database: String = "auth_v1",
    var collection: String = "tokens",
    var deleted: Option[String] = None,
    var servers: String = "localhost") {

    def deletedCollection(): String = {
      deleted.getOrElse( collection + "_deleted" )
    }

    def mongoConfig(): Configuration = {
      Configuration.parse("servers = %s".format(mongoServers()))
    }

    def mongoServers(): String = {
      val s = servers.trim
      if(s.startsWith("[") && s.endsWith("]")) {
        s
      } else {
        "[" + s + "]"
      }
    }
  }
}

object CSVToJSONConverter {
  def convert(file: String, delimeter: Char = ',', timestampConversion: Boolean = false): Iterator[JValue] = new Iterator[JValue] {

    private val reader = new CSVReader(new FileReader(file), delimeter)
    private val header = reader.readNext 
    private var line = reader.readNext

    def hasNext(): Boolean = line != null 

    def next(): JValue = {
      val result = JObject(header.zip(line).map{ 
        case (k, v) => JField(k, parse(v, timestampConversion))
      }.toList)
      line = reader.readNext
      result
    }
  }

  private val Timestamp = """^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}\.\d{3})\d{0,3}$""".r

  def parse(s: String, ts: Boolean): JValue = {
    try {
      JsonParser.parse(s)
    } catch {
      case ex =>
        s match {
          case Timestamp(d, t) if (ts) => JString("%sT%sZ".format(d,t))
          case s                       => JString(s)
        }
    }
  }
}
