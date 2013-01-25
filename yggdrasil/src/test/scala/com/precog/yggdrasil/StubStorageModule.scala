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
package com.precog.yggdrasil

import actor._
import metadata._
import util._
import com.precog.util._
import SValue._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.util._
import com.precog.common.json._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json._

import scalaz._
import scalaz.effect._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class StubStorageMetadata[M[+_]](projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata])(implicit val M: Monad[M]) extends StorageMetadata[M] {
  val source = new TestMetadataStorage(projectionMetadata)
  def findChildren(path: Path) = M.point(source.findChildren(path))
  def findSelectors(path: Path) = M.point(source.findSelectors(path))
  def findProjections(path: Path, selector: CPath) = M.point {
    projectionMetadata.collect {
      case (descriptor, _) if descriptor.columns.exists { case ColumnDescriptor(p, s, _, _) => p == path && s == selector } => 
        (descriptor, ColumnMetadata.Empty)
    }
  }

  def findPathMetadata(path: Path, selector: CPath) = M.point(source.findPathMetadata(path, selector).unsafePerformIO)
}

trait StubProjectionModule[M[+_], Key, Block] extends ProjectionModule[M, Key, Block] with StorageMetadataSource[M] { self =>
  implicit def M: Monad[M]

  protected def projections: Map[ProjectionDescriptor, Projection]
  protected def projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = projections.keys.map(pd => (pd, ColumnMetadata.Empty)).toMap
  protected def metadata = new StubStorageMetadata(projectionMetadata)(M)

  class ProjectionCompanion extends ProjectionCompanionLike[M] {
    def apply(descriptor: ProjectionDescriptor) = M.point(projections(descriptor))
  }

  def userMetadataView(apiKey: APIKey) = new UserMetadataView[M](apiKey, new UnrestrictedAccessControl(), metadata)
}
