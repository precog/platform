package com.precog.shard.util

import com.precog.common._
import com.precog.util._
import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._

import com.precog.util.FilesystemFileOps

import java.io.File

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.util.Timeout
import akka.util.Duration

import java.util.concurrent.atomic.AtomicInteger

import org.streum.configrity.Configuration

import blueeyes.json.JPath
import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._

import scalaz.effect.IO

object ShardTestInit extends App {

  val dir = new File("./data") 
  dir.mkdirs

  object shard extends StandaloneActorEcosystem[IterableDataset] with ActorYggShard[IterableDataset] with LevelDBProjectionsActorModule {
    class YggConfig(val config: Configuration) extends BaseConfig with ProductionActorConfig 

    val yggConfig = new YggConfig(Configuration.parse("precog.storage.root = " + dir.getName))
    val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, new FilesystemFileOps {}).unsafePerformIO
    val initialCheckpoint = None
    val accessControl = new UnlimitedAccessControl()(ExecutionContext.defaultExecutionContext(actorSystem))
  }

  def usage() {
    println("usage: command [{datapath}={filename}]")
    System.exit(1)
  }

  private val seqId = new AtomicInteger(0) 

  def run(loads: Array[String]) {
    Await.result(shard.actorsStart, Duration(30, "seconds"))
    val timeout = Timeout(30000) 
    loads.foreach{ insert(_, timeout) }
    Await.result(shard.actorsStop, Duration(30, "seconds"))
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
          val fut = shard.storeBatch(elements.map{ value =>
            println(Printer.compact(Printer.render(value)))
            EventMessage(EventId(0, seqId.getAndIncrement), Event(Path(path), "root", value, emptyMetadata))
          }, timeout)
          Await.result(fut, Duration(30, "seconds")) 
        case single           =>
          val fut = shard.store(EventMessage(EventId(0, seqId.getAndIncrement), Event(Path(path), "root", single, emptyMetadata)), timeout)
          Await.result(fut, Duration(30, "seconds")) 
      }
    } except {
      err => println(err); IO(())
    } unsafePerformIO
  }

  if(args.length == 0) usage else run(args)

}
