package com.precog.shard.jdbc

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.precog.common._

import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._

import com.weiglewilczek.slf4s.Logging

class JDBCStorageMetadataSource(dbMap: Map[String, String])(implicit asyncContext: ExecutionContext) extends StorageMetadataSource[Future] {
  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = new JDBCStorageMetadata(dbMap)
}

class JDBCStorageMetadata(dbMap: Map[String, String])(implicit asyncContext: ExecutionContext) extends StorageMetadata[Future] with Logging {
  implicit val M = new FutureMonad(asyncContext) 

  // FIXME: Actually implement these for JDBC
  def findDirectChildren(path: Path): Future[Set[Path]] = {
    logger.warn("Path globs will be supported in a future release of Precog for JDBC")
    Promise.successful(Set())
  }

  def findSize(path: Path) = Promise.successful(0L)

  def findSelectors(path: Path): Future[Set[CPath]] = Promise.successful(Set())

  def findStructure(path: Path, selector: CPath) = Promise.successful(PathStructure.Empty)

  def currentVersion(path: Path) = Promise.successful(None)
  def currentAuthorities(path: Path) = Promise.successful(None)
}
