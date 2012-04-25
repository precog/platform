package com.precog.yggdrasil 

import metadata.MetadataView
import com.precog.common._

import akka.dispatch.Future 
import akka.util.Timeout

trait YggShardComponent {
  type Dataset
  type Storage <: YggShard[Dataset]
  def storage: Storage
}

trait YggShard[Dataset] { self =>
  def userMetadataView(uid: String): MetadataView
  def projection(descriptor: ProjectionDescriptor, timeout: Timeout): Future[Projection[Dataset]]
  def store(msg: EventMessage, timeout: Timeout): Future[Unit] = storeBatch(Vector(msg), timeout) 
  def storeBatch(msgs: Seq[EventMessage], timeout: Timeout): Future[Unit]
}
