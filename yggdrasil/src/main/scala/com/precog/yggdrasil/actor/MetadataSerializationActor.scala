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

import com.precog.common.util._

import akka.actor.Actor

import scalaz.effect._
import scalaz.Scalaz._

import com.weiglewilczek.slf4s.Logging

class MetadataSerializationActor(checkpoints: YggCheckpoints, metadataIO: MetadataIO) extends Actor with Logging {
  def receive = {
    case SaveMetadata(metadata, messageClock) => 
      logger.debug("Syncing metadata")
      metadata.toList.map {
        case (pd, md) => metadataIO(pd, md)
      }.sequence[IO, Unit].map(_ => ()).unsafePerformIO
      logger.debug("Registering metadata checkpoint: " + messageClock)
      checkpoints.metadataPersisted(messageClock)
  }
}

sealed trait MetadataSerializationAction

case class SaveMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], messageClock: VectorClock) extends MetadataSerializationAction
