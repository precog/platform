package com.precog
package pandora

import common.VectorCase
import common.kafka._
import common.security._

import daze._
import daze.memoization._
import daze.util._

import pandora._

import quirrel._
import quirrel.emitter._
import quirrel.parser._
import quirrel.typer._

import yggdrasil._
import yggdrasil.actor._
import yggdrasil.metadata._
import yggdrasil.serialization._
import muspelheim._

import com.precog.util.FilesystemFileOps

import org.specs2.mutable._
  
import akka.dispatch.Await
import akka.util.Duration

import java.io.File

import scalaz._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

class PlatformSpecs extends ParseEvalStackSpecs { platformSpecs =>
  type Storage = LevelDBActorYggShard[platformSpecs.YggConfig]
                                               
  val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, new FilesystemFileOps {}).unsafePerformIO

  object storage extends LevelDBActorYggShard[platformSpecs.YggConfig](platformSpecs.yggConfig, metadataStorage) {
    val accessControl = new UnlimitedAccessControl()(ExecutionContext.defaultExecutionContext(actorSystem))
  }

  override def startup() {
    // start storage shard 
    Await.result(storage.start(), controlTimeout)
  }
  
  override def shutdown() {
    // stop storage shard
    Await.result(storage.stop(), controlTimeout)
    
    actorSystem.shutdown()
  }
}
