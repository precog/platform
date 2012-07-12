package com.precog.yggdrasil
package iterable

import actor._
import metadata._

import com.precog.common._
import com.precog.common.security._

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.dispatch.{Await,Dispatcher,ExecutionContext,Future,Promise, Futures}
import akka.pattern.ask
import akka.pattern.gracefulStop
import akka.util.Timeout
import akka.util.duration._

import scalaz.effect._

import com.weiglewilczek.slf4s.Logging

trait LevelDBActorYggShardModule extends ShardSystemActorModule[IterableDataset[Seq[CValue]]] {
  abstract class LevelDBActorYggShard(storage: MetadataStorage)(implicit val actorSystem: ActorSystem) extends ActorYggShard[IterableDataset[Seq[CValue]]] {
    def this(storage: MetadataStorage, systemName: String) = this(storage)(ActorSystem(systemName))

    private var shardSystemActor0: ActorRef = _
    def shardSystemActor = shardSystemActor0
    
    def start() = Future {
      shardSystemActor0 = actorSystem.actorOf(Props(new ShardSystemActor[IterableDataset[Seq[CValue]]](storage)), "shardSystem")
      true
    }

    def stop() = {
      import yggConfig.stopTimeout

      for {
        _ <- shardSystemActor ? ShutdownSystem
        shutdownResult <- gracefulStop(shardSystemActor0, stopTimeout.duration)
      } yield shutdownResult
    }
  }
}

// vim: set ts=4 sw=4 et:
