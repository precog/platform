package com.precog.shard
package util

import com.precog.util._
import com.precog.common._
import com.precog.common.security._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.memoization._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._

import blueeyes.json.JPath
import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Timeout
import akka.util.Duration

import org.streum.configrity.Configuration

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scalaz.effect.IO

object ShardTestInit extends App with JDBMProjectionModule with SystemActorStorageModule with StandaloneShardSystemActorModule {

  val dir = new File("./data") 
  dir.mkdirs

  class YggConfig(val config: Configuration) extends BaseConfig with StandaloneShardSystemConfig

  val yggConfig = new YggConfig(Configuration.parse("precog.storage.root = " + dir.getName))

  val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, FilesystemFileOps).unsafePerformIO

  val actorSystem = ActorSystem("shard-test-init")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  class Storage extends SystemActorStorageLike(metadataStorage) {
    val accessControl = new UnlimitedAccessControl[Future]()
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
  }

  def usage() {
    println("usage: command [{datapath}={filename}]")
    System.exit(1)
  }

  private val seqId = new AtomicInteger(0) 

  def run(loads: Array[String]) {
    Await.result(storage.start(), Duration(30, "seconds"))
    val timeout = Timeout(30000) 
    loads.foreach{ insert(_, timeout) }
    Await.result(storage.stop(), Duration(30, "seconds"))
  }

  def insert(load: String, timeout: Timeout) {
    val parts = load.split("=")

    val filename = parts(1)
    val path = parts(0)

    IOUtils.readFileToString(new File(filename)).map { data =>
      val json = JsonParser.parse(data)

      val emptyMetadata: Map[JPath, Set[UserMetadata]] = Map.empty

      json match {
        case JArray(elements) => 
          val fut = storage.storeBatch(elements.map{ value =>
            println(Printer.compact(Printer.render(value)))
            EventMessage(EventId(0, seqId.getAndIncrement), Event(Path(path), "root", value, emptyMetadata))
          }, timeout)
          Await.result(fut, Duration(30, "seconds")) 
        case single           =>
          val fut = storage.store(EventMessage(EventId(0, seqId.getAndIncrement), Event(Path(path), "root", single, emptyMetadata)), timeout)
          Await.result(fut, Duration(30, "seconds")) 
      }
    } except {
      err => println(err); IO(())
    } unsafePerformIO
  }

  if(args.length == 0) usage else run(args)

}
