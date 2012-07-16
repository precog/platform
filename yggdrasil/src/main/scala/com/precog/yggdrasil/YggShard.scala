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

import metadata.StorageMetadata
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

import scalaz.effect._
import scalaz.syntax.bind._

trait YggShardComponent {
  type ProjectionImpl <: Projection
  type Storage <: YggShard[ProjectionImpl]
  def storage: Storage
}

trait YggShardMetadata {
  def userMetadataView(uid: String): StorageMetadata
}

trait YggShard[+P <: Projection] extends YggShardMetadata { self =>
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(P, Release)]
  def store(msg: EventMessage, timeout: Timeout): Future[Unit] = storeBatch(Vector(msg), timeout) 
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit]
}

class Release(private var _release: IO[Unit]) { self => 
  def release: IO[Unit] = _release

  def += (action: IO[Unit]): self.type = {
    synchronized {
      _release = self.release >> action
    }
    self
  }
}

