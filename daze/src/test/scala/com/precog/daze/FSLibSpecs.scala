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
package com.precog.daze

import com.precog.bytecode._
import com.precog.common.Path
import com.precog.common.json._
import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import blueeyes.json._

import org.joda.time.DateTime
import org.specs2.mutable.Specification

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._

trait FSLibSpecs[M[+_]] extends Specification with FSLibModule[M] with TestColumnarTableModule[M] {
  import trans._
  import constants._

  val library = new FSLib {}
  import library._

  implicit def M: Monad[M] with Copointed[M]

  class YggConfig extends IdSourceConfig with ColumnarTableModuleConfig {
    val maxSliceSize = 10
    val smallSliceSize = 3
    val idSource = new FreshAtomicIdSource
  }

  lazy val yggConfig = new YggConfig

  lazy val projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = Map(
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo/bar1/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo/bar2/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo/bar2/baz/quux2"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo2/bar1/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty
  )

  def userMetadataView(apiKey: APIKey): StorageMetadata[M] = new StubStorageMetadata(projectionMetadata)

  def pathTable(path: String) = {
    Table.constString(Set(path)).transform(WrapObject(Leaf(Source), TransSpecModule.paths.Value.name))
  }

  def runExpansion(table: Table): List[JValue] = {
    expandGlob(table, EvaluationContext("", Path.Root, new DateTime())).map(_.transform(SourceValue.Single)).flatMap(_.toJson).copoint.toList
  }

  "path globbing" should {
    "not alter un-globbed paths" in {
      val table = pathTable("/foo/bar/baz/")
      val expected: List[JValue] = List(JString("/foo/bar/baz/"))
      runExpansion(table) must_== expected
    }
    
    "expand a leading glob" in {
      val table = pathTable("/*/bar1")
      val expected: List[JValue] = List(JString("/foo/bar1/"), JString("/foo2/bar1/"))
      runExpansion(table) must_== expected
    }

    "expand a trailing glob" in {
      val table = pathTable("/foo/*")
      val expected: List[JValue] = List(JString("/foo/bar1/"), JString("/foo/bar2/"))
      runExpansion(table) must_== expected
    }

    "expand an internal glob and filter" in {
      val table = pathTable("/foo/*/baz/quux1")
      val expected: List[JValue] = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"))
      runExpansion(table) must_== expected
    }

    "expand multiple globbed segments" in {
      val table = pathTable("/foo/*/baz/*")
      val expected: List[JValue] = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"), JString("/foo/bar2/baz/quux2/"))
      runExpansion(table) must_== expected
    }
  }
}

object FSLibSpecs extends FSLibSpecs[test.YId] with test.YIdInstances
