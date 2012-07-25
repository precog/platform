package com.precog.yggdrasil 

import metadata.StorageMetadata
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

import scalaz._
import scalaz.effect._
import scalaz.syntax.bind._
import scala.annotation.unchecked._

trait StorageModule[M[+_]] {
  type Projection <: ProjectionLike
  type Storage <: StorageLike
  def storage: Storage

  trait StorageLike extends StorageMetadataSource[M] { self =>
    def projection(descriptor: ProjectionDescriptor): M[(Projection, Release)]
    def storeBatch(msgs: Seq[EventMessage]): M[Unit]
    def store(msg: EventMessage): M[Unit] = storeBatch(Vector(msg))
  }
}

trait StorageMetadataSource[M[+_]] {
  def userMetadataView(uid: String): StorageMetadata[M]
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

