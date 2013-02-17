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

import com.precog.common.accounts.AccountFinder
import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._

import com.precog.util.PrecogUnit

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.dispatch.{Await,Dispatcher,ExecutionContext,Future,Promise, Futures}
import akka.pattern.ask
import akka.pattern.gracefulStop
import akka.util.Timeout
import akka.util.duration._

import scalaz._
import scalaz.effect._

import com.weiglewilczek.slf4s.Logging

// trait ActorStorageModuleConfig {
//   def metadataTimeout: Timeout
// }
// 
// trait ActorStorageModule extends StorageModule[Future] with YggConfigComponent {
//   implicit def M: Monad[Future]
// 
//   type YggConfig <: ActorStorageModuleConfig
// 
//   def accessControl: AccessControl[Future]
// 
//   class ActorStorageLike(actorSystem: ActorSystem, ingestSupervisor: ActorRef, metadataActor: ActorRef)(implicit executor: ExecutionContext) extends StorageLike[Future] with Logging {
//     val storageMetadata: StorageMetadata[Future] = new ActorStorageMetadata(metadataActor, yggConfig.metadataTimeout)
// 
//     def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = new UserMetadataView(apiKey, accessControl, storageMetadata)
//     
//     def storeBatch(msgs: Seq[EventMessage]): Future[PrecogUnit] = {
//       implicit val storageTimeout: Timeout = Timeout(300 seconds)
// 
//       val result = Promise.apply[BatchComplete]
//       val notifier = actorSystem.actorOf(Props(new BatchCompleteNotifier(result)))
//       val batchHandler = actorSystem.actorOf(Props(new BatchHandler(notifier, null, YggCheckpoint.Empty, Timeout(120000))))
// 
//       ingestSupervisor.tell(DirectIngestData(msgs), batchHandler)
// 
//       for (complete <- result) yield {
//         val checkpoint = complete.checkpoint
//         logger.debug("Batch store complete: " + complete)
// 
//         logger.debug("Sending metadata updates")
//         metadataActor ! IngestBatchMetadata(complete.updatedProjections, checkpoint.messageClock, Some(checkpoint.offset))
//         PrecogUnit
//       }
//     }
//   }
// }
