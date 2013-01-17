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
import com.precog.common.ingest._
import com.precog.util.PrecogUnit
import com.precog.common.security._

import akka.dispatch.Future 

import scalaz._
import scalaz.effect._
import scalaz.syntax.bind._
import scalaz.syntax.monad._
import scala.annotation.unchecked._

trait StorageModule[M[+_]] {
  type Projection <: ProjectionLike
  type Storage <: StorageLike[M]
  def storage: Storage

  trait StorageLike[M[+_]] extends StorageMetadataSource[M] { self =>
    def projection(descriptor: ProjectionDescriptor): M[(Projection, Release)]

/*
    def liftM[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]): StorageLike[({ type λ[+α] = T[M, α] })#λ] = new Lifted[T]

    protected class Lifted[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]) extends StorageLike[({ type λ[+α] = T[M, α] })#λ] {
      def projection(descriptor: ProjectionDescriptor) = self.projection(descriptor).liftM[T]
      def userMetadataView(apiKey: APIKey) = self.userMetadataView(apiKey).liftM[T]
    }
    */
  }

  trait StorageWritable[M[+_]] extends StorageLike[M] { self =>
    def storeBatch(msgs: Seq[EventMessage]): M[PrecogUnit]
    def store(msg: EventMessage): M[PrecogUnit] = storeBatch(Vector(msg))

/*
    override def liftM[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]): StorageWritable[({ type λ[+α] = T[M, α] })#λ] = 
      new Lifted[T] with StorageWritable[({ type λ[+α] = T[M, α] })#λ] {
        def storeBatch(msgs: Seq[EventMessage]) = self.storeBatch(msgs).liftM[T]
        override def store(msg: EventMessage) = self.store(msg).liftM[T]
      }
      */
  }
}

trait StorageMetadataSource[M[+_]] {
  def userMetadataView(apiKey: APIKey): StorageMetadata[M]
}

class Release(private var _release: IO[PrecogUnit]) { self => 
  def release: IO[PrecogUnit] = _release

  def += (action: IO[PrecogUnit]): self.type = {
    synchronized {
      _release = self.release >> action
    }
    self
  }
}

