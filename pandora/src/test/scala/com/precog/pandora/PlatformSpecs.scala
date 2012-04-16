package com.precog
package pandora

import common.VectorCase
import common.kafka._

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
import yggdrasil.serialization._
import muspelheim._

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
  trait Storage extends ActorYggShard[IterableDataset] with StandaloneActorEcosystem {
    type YggConfig = platformSpecs.YggConfig
    lazy val yggConfig = platformSpecs.yggConfig
    lazy val yggState = shardState 
  }
  
  object storage extends Storage

  override def startup() {
    // start storage shard 
    Await.result(storage.actorsStart, controlTimeout)
  }
  
  override def shutdown() {
    // stop storage shard
    Await.result(storage.actorsStop, controlTimeout)
    
    actorSystem.shutdown()
  }
}
