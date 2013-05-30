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
package vfs

import actor._
import execution._
import metadata._
import util._
import SValue._
import ResourceError._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.util._

import com.precog.util._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json._

import scalaz._
import scalaz.effect._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class StubVFSMetadata[M[+_]](projectionMetadata: Map[Path, Map[ColumnRef, Long]])(implicit M: Monad[M]) extends VFSMetadata[M]{
  def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[Path]] = EitherT.right {
    M point {
      projectionMetadata.keySet collect {
        case key if key.isChildOf(path) => Path(key.components(path.length))
      }
    }
  }

  private def getPathMeta(path: Path): EitherT[M, ResourceError, Map[ColumnRef, Long]] = EitherT {
    M.point(projectionMetadata.get(path) \/> NotFound("No metadata found for path %s".format(path.path)))
  }

  def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = {
    for {
      types <- getPathMeta(path) map { _ collect {
        case (ColumnRef(`property`, ctype), count) => (ctype, count)
      } }

      children <- getPathMeta(path) map { _ flatMap {
        case t @ (ColumnRef(s, ctype), count) =>
          if (s.hasPrefix(property)) s.take(property.length + 1) else None
      } }
    } yield PathStructure(types, children.toSet)
  }

  def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long] = {
    getPathMeta(path) map(_.values.max)
  }
}
