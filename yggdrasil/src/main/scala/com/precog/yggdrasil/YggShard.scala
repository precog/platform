package com.precog.yggdrasil 

import metadata.MetadataView
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

import scalaz.effect._
import scalaz.syntax.bind._

trait YggShardComponent[Dataset] {
  type Storage <: YggShard[Dataset]
  def storage: Storage
}

trait YggShardMetadata {
  def userMetadataView(uid: String): MetadataView
}

trait YggShard[Dataset] extends YggShardMetadata { self =>
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[(Projection[Dataset], Release)]
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

