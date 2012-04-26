package com.precog.yggdrasil 

import metadata.MetadataView
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

trait YggShardComponent[Dataset] {
  type Storage <: YggShard[Dataset]
  def storage: Storage
}

trait YggShardMetadata {
  def userMetadataView(uid: String): MetadataView
}

trait YggShard[Dataset] extends YggShardMetadata { self =>
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection[Dataset]]
  def store(msg: EventMessage, timeout: Timeout): Future[Unit] = storeBatch(Vector(msg), timeout) 
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit]
}
