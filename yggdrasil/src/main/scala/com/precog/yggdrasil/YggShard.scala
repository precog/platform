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

