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

    def liftM[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]): StorageLike[({ type λ[+α] = T[M, α] })#λ] = new Lifted[T]

    protected class Lifted[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]) extends StorageLike[({ type λ[+α] = T[M, α] })#λ] {
      def projection(descriptor: ProjectionDescriptor) = self.projection(descriptor).liftM[T]
      def userMetadataView(apiKey: APIKey) = self.userMetadataView(apiKey).liftM[T]
    }
  }

  trait StorageWritable[M[+_]] extends StorageLike[M] { self =>
    def storeBatch(msgs: Seq[EventMessage]): M[PrecogUnit]
    def store(msg: EventMessage): M[PrecogUnit] = storeBatch(Vector(msg))

    override def liftM[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]): StorageWritable[({ type λ[+α] = T[M, α] })#λ] = 
      new Lifted[T] with StorageWritable[({ type λ[+α] = T[M, α] })#λ] {
        def storeBatch(msgs: Seq[EventMessage]) = self.storeBatch(msgs).liftM[T]
        override def store(msg: EventMessage) = self.store(msg).liftM[T]
      }
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

