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
import scala.annotation.unchecked._

trait StorageLike[M[+_], +Projection] extends StorageMetadataSource[M] { self =>
    def projection(descriptor: ProjectionDescriptor): M[(Projection, Release)]
  }

  trait StorageWritable extends StorageLike {
    def storeBatch(msgs: Seq[EventMessage]): M[PrecogUnit]
    def store(msg: EventMessage): M[PrecogUnit] = storeBatch(Vector(msg))

  def liftM[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]) = new StorageLike[({ type λ[+α] = T[M, α] })#λ, Projection] {
    import scalaz.syntax.monad._

    def projection(descriptor: ProjectionDescriptor) = self.projection(descriptor).liftM[T]
    def storeBatch(msgs: Seq[EventMessage]) = self.storeBatch(msgs).liftM[T]
    override def store(msg: EventMessage) = self.store(msg).liftM[T]
    def userMetadataView(apiKey: APIKey) = self.userMetadataView(apiKey).liftM[T]
  }
}

trait StorageModule[M[+_]] {
  type Projection <: ProjectionLike
  type Storage <: StorageLike[M, Projection]
  def storage: Storage
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

