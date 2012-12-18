package com.precog.shard.mongo

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.mongodb.Mongo

import com.precog.common._
import com.precog.common.json._
import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._

import com.weiglewilczek.slf4s.Logging

class MongoStorageMetadataSource(mongo: Mongo)(implicit asyncContext: ExecutionContext) extends StorageMetadataSource[Future] {
  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = new MongoStorageMetadata(mongo)
}

class MongoStorageMetadata(mongo: Mongo)(implicit asyncContext: ExecutionContext) extends StorageMetadata[Future] with Logging {
  implicit val M = AkkaTypeClasses.futureApplicative(asyncContext) 

  // FIXME: Actually implement these for Mongo
  def findChildren(path: Path): Future[Set[Path]] = {
    logger.warn("Path globs will be supported in a future release of Precog for MongoDB")
    Promise.successful(Set())
  }

  def findSelectors(path: Path): Future[Set[CPath]] = Promise.successful(Set())
  def findProjections(path: Path, selector: CPath): Future[Map[ProjectionDescriptor, ColumnMetadata]]  = Promise.successful(Map())
  def findPathMetadata(path: Path, selector: CPath): Future[PathRoot]  = Promise.successful(PathRoot(Set()))
}
