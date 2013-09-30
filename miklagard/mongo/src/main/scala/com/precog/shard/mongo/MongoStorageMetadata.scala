/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.bifrost.mongo

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.mongodb.Mongo

import com.precog.common._

import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._

import com.weiglewilczek.slf4s.Logging

class MongoStorageMetadataSource(mongo: Mongo)(implicit asyncContext: ExecutionContext) extends StorageMetadataSource[Future] {
  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = new MongoStorageMetadata(mongo)
}

class MongoStorageMetadata(mongo: Mongo)(implicit asyncContext: ExecutionContext) extends StorageMetadata[Future] with Logging {
  implicit val M = new FutureMonad(asyncContext) 

  // FIXME: Actually implement these for Mongo
  def findDirectChildren(path: Path): Future[Set[Path]] = {
    logger.warn("Path globs will be supported in a future release of Precog for MongoDB")
    Promise.successful(Set())
  }

  def findSize(path: Path) = Promise.successful(0L)

  def findSelectors(path: Path): Future[Set[CPath]] = Promise.successful(Set())

  def findStructure(path: Path, selector: CPath) = Promise.successful(PathStructure.Empty)

  def currentVersion(path: Path) = Promise.successful(None)
  def currentAuthorities(path: Path) = Promise.successful(None)
}
