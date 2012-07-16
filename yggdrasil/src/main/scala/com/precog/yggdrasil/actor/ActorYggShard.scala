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

trait ActorYggShardComponent extends YggShardComponent {
  trait ActorYggShard extends YggShard[ProjectionImpl] with Logging {
    def accessControl: AccessControl
    protected implicit def actorSystem: ActorSystem
    def shardSystemActor: ActorRef

    def start(): Future[Boolean]
    def stop(): Future[Boolean]

    private lazy implicit val dispatcher = actorSystem.dispatcher
    private lazy val metadata: StorageMetadata = new ActorStorageMetadata(shardSystemActor)
    
    def userMetadataView(uid: String): StorageMetadata = {
      implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
      new UserMetadataView(uid, accessControl, metadata)
    }
    
    def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(ProjectionImpl, Release)] = {
      logger.debug("Obtain projection for " + descriptor)
      implicit val ito = timeout 

      (for (ProjectionAcquired(projection) <- (shardSystemActor ? AcquireProjection(descriptor))) yield {
        logger.debug("  projection obtained")
        (projection.asInstanceOf[ProjectionImpl], new Release(IO(shardSystemActor ! ReleaseProjection(descriptor))))
      }) onFailure {
        case e => logger.error("Error acquiring projection: " + descriptor, e)
      }
    }
    
    def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
      implicit val ito = timeout
      val result = Promise.apply[BatchComplete]
      val notifier = actorSystem.actorOf(Props(new BatchCompleteNotifier(result)))
      val batchHandler = actorSystem.actorOf(Props(new BatchHandler(notifier, null, YggCheckpoint.Empty, Timeout(120000))))
      shardSystemActor.tell(DirectIngestData(msgs), batchHandler)

      result map { complete =>
        logger.debug("Batch store complete: " + complete)
      }
    }
  }
}
