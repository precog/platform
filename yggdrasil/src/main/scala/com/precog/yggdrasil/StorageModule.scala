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

trait StorageModule[M[+_]] {
  type Projection <: ProjectionLike
  type Storage <: StorageLike
  def storage: Storage

  trait StorageLike extends StorageMetadataSource[M] { self =>
    def projection(descriptor: ProjectionDescriptor): M[(Projection, Release)]
    def storeBatch(msgs: Seq[EventMessage]): M[PrecogUnit]
    def store(msg: EventMessage): M[PrecogUnit] = storeBatch(Vector(msg))
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

