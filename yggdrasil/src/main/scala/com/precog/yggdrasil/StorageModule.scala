package com.precog.yggdrasil 

import metadata.StorageMetadata
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

import scalaz._
import scalaz.effect._
import scalaz.syntax.bind._

trait StorageModule[M[+_]] {
  type Projection <: ProjectionLike
  type Storage <: StorageLike[Projection, M]
  def storage: Storage
}

trait StorageMetadataSource[M[+_]] {
  def userMetadataView(uid: String): StorageMetadata[M]
}

trait StorageLike[+Projection <: ProjectionLike, M[+_]] extends StorageMetadataSource[M] { self =>
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): M[(Projection, Release)]
  def store(msg: EventMessage, timeout: Timeout): M[Unit] = storeBatch(Vector(msg), timeout) 
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): M[Unit]
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

