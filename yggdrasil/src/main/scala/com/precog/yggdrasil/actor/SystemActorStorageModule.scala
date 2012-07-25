package com.precog.yggdrasil
package actor

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

trait SystemActorStorageModule extends ActorStorageModule with ShardSystemActorModule {
  type Storage <: SystemActorStorageLike
  //type YggConfig <: ShardSystemActorModule#YggConfig with ActorStorageModule#YggConfig

  abstract class SystemActorStorageLike(metadataStorage: MetadataStorage) extends ActorStorageLike {
    private var shardSystemActor0: ActorRef = _
    def shardSystemActor = shardSystemActor0
    
    def start() = Future {
      shardSystemActor0 = actorSystem.actorOf(Props(new ShardSystemActor(metadataStorage)), "shardSystem")
      true
    }

    def stop() = {
      implicit val stopTimeout: Timeout = yggConfig.stopTimeout

      for {
        _ <- shardSystemActor ? ShutdownSystem
        shutdownResult <- gracefulStop(shardSystemActor0, stopTimeout.duration)
      } yield shutdownResult
    }
  }
}

// vim: set ts=4 sw=4 et:
