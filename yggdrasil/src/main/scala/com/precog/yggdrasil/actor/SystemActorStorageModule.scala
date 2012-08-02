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
