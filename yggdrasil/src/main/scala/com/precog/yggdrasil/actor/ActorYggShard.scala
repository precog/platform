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

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout

trait ActorYggShard extends YggShard with ActorEcosystem {
  
  def yggState: YggState

  lazy implicit val dispatcher = actorSystem.dispatcher

  private lazy val metadata: StorageMetadata = new ActorStorageMetadata(metadataActor)
  
  def userMetadataView(uid: String): MetadataView = {
    implicit val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
    new UserMetadataView(uid, new UnlimitedAccessControl, metadata)
  }
  
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection] = {
    implicit val ito = timeout 
    (projectionsActor ? AcquireProjection(descriptor)) flatMap {
      case ProjectionAcquired(actorRef) =>
        projectionsActor ! ReleaseProjection(descriptor)
        (actorRef ? ProjectionGet).mapTo[Projection]
      
      case ProjectionError(err) =>
        sys.error("Error acquiring projection actor: " + err)
    }
  }
  
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit] = {
    implicit val ito = timeout
    (routingActor ? Messages(msgs)) map { _ => () }
  }
  
}

