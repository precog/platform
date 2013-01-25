package com.precog.yggdrasil 

import metadata.StorageMetadata
import com.precog.common._
import com.precog.common.ingest._
import com.precog.util.PrecogUnit
import com.precog.common.security._

import scalaz._
import scalaz.effect._
import scalaz.syntax.bind._
import scalaz.syntax.monad._
import scala.annotation.unchecked._

trait StorageModule[M[+_]] {
  type Storage <: StorageLike[M]

  def storage: Storage
}

trait StorageMetadataSource[M[+_]] {
  def userMetadataView(apiKey: APIKey): StorageMetadata[M]
}

trait StorageLike[M[+_]] extends StorageMetadataSource[M] { self =>
  def storeBatch(msgs: Seq[EventMessage]): M[PrecogUnit]
  def store(msg: EventMessage): M[PrecogUnit] = storeBatch(Vector(msg))

  def liftM[T[_[+_], +_]](implicit T: Hoist[T], M: Monad[M]) = new StorageLike[({ type λ[+α] = T[M, α] })#λ] {
    def userMetadataView(apiKey: APIKey) = self.userMetadataView(apiKey).liftM[T]
    def storeBatch(msgs: Seq[EventMessage]) = self.storeBatch(msgs).liftM[T]
    override def store(msg: EventMessage) = self.store(msg).liftM[T]
  }
}

