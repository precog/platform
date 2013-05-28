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
package com.precog.shard
package service


import com.precog.common._
import com.precog.yggdrasil._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.vfs._

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

import org.specs2.mutable._

abstract class BrowseServiceSpecs[M[+_]](implicit val M: Monad[M] with Comonad[M]) extends Specification {
  def colSizeMetadata(descriptor: ColumnRef, size: Long): ColumnMetadata = Map(
    descriptor -> Map(StringValueStats -> StringValueStats(size, "a", "z"))    
  )

  lazy val projectionMetadata: Map[Path, Map[ColumnRef, Long]] = Map(
    Path("/foo/bar1/baz/quux1") -> Map(ColumnRef(CPath(), CString) -> 10L),
    Path("/foo/bar2/baz/quux1") -> Map(ColumnRef(CPath(), CString) -> 20L),
    Path("/foo/bar2/baz/quux2") -> Map(ColumnRef(CPath(), CString) -> 30L),
    Path("/foo2/bar1/baz/quux1") -> Map(ColumnRef(CPath(), CString) -> 40L),
    Path("/foo/bar/") -> Map(ColumnRef(CPath(".bar"), CLong) -> 50, ColumnRef(CPath(".baz"), CLong) -> 60L)
  )

  val metadata = new StubVFSMetadata[M](projectionMetadata)
  val client = new BrowseSupport(metadata)

  "browse" should {
    "find child paths" in {
      client.browse("", Path("/foo/")).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case JArray(results) => results must haveTheSameElementsAs(JString("bar/") :: JString("bar1/") :: JString("bar2/") :: Nil)
      }
    }
  }

  "structure" should {
    "find correct node information" in {
      client.structure("", Path("/foo/bar"), CPath.Identity).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case result => result must_== JObject("children" -> JArray(JString(".bar") :: JString(".baz") :: Nil), "types" -> JObject())
      }
    }

    "find correct leaf types" in {
      client.structure("", Path("/foo/bar"), CPath("bar")).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case result => result must_== JObject("children" -> JArray(), "types" -> JObject("Number" -> JNum(50)))
      }
    }
  }
}

object BrowseServiceSpecs extends BrowseServiceSpecs[Need]

// vim: set ts=4 sw=4 et:
