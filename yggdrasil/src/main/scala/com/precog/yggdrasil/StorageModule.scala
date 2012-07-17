package com.precog.yggdrasil 

import metadata.StorageMetadata
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

import scalaz.effect._
import scalaz.syntax.bind._

trait StorageModule {
  type Projection <: ProjectionLike
  type Storage <: StorageLike[Projection]
  def storage: Storage
}

trait StorageMetadataSource {
  def userMetadataView(uid: String): StorageMetadata
}

trait StorageLike[+P <: ProjectionLike] extends StorageMetadataSource { self =>
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

