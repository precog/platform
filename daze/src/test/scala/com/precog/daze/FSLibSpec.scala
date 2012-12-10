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

trait FSLibSpec[M[+_]] extends Specification 
  with FSLib[M]
  with TestColumnarTableModule[M] {

  implicit def M: Monad[M] with Copointed[M]

  class YggConfig extends IdSourceConfig with ColumnarTableModuleConfig {
    val maxSliceSize = 10
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }

  val yggConfig = new YggConfig

  val projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = Map(
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo/bar1/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo/bar2/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo/bar2/baz/quux2"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnDescriptor(Path("/foo2/bar1/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty
  )

  object storage extends StorageMetadataSource[M] {
    def userMetadataView(apiKey: APIKey): StorageMetadata[M] = new StubStorageMetadata(projectionMetadata)
  }

  "path globbing" should {
    "not alter un-globbed paths" in {
      val table = Table.constString(Set(CString("/foo/bar/baz/")))
      val expected = List(JString("/foo/bar/baz/"))
      expandGlob(table, EvaluationContext("", Path.Root, new DateTime())).flatMap(_.toJson).copoint.toList must_== expected
    }
    
    "expand a leading glob" in {
      val table = Table.constString(Set(CString("/*/bar1")))
      val expected = List(JString("/foo/bar1/"), JString("/foo2/bar1/"))
      expandGlob(table, EvaluationContext("", Path.Root, new DateTime())).flatMap(_.toJson).copoint.toList must_== expected
    }

    "expand a trailing glob" in {
      val table = Table.constString(Set(CString("/foo/*")))
      val expected = List(JString("/foo/bar1/"), JString("/foo/bar2/"))
      expandGlob(table, EvaluationContext("", Path.Root, new DateTime())).flatMap(_.toJson).copoint.toList must_== expected
    }

    "expand an internal glob and filter" in {
      val table = Table.constString(Set(CString("/foo/*/baz/quux1")))
      val expected = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"))
      expandGlob(table, EvaluationContext("", Path.Root, new DateTime())).flatMap(_.toJson).copoint.toList must_== expected
    }

    "expand multiple globbed segments" in {
      val table = Table.constString(Set(CString("/foo/*/baz/*")))
      val expected = List(JString("/foo/bar1/baz/quux1/"), JString("/foo/bar2/baz/quux1/"), JString("/foo/bar2/baz/quux2/"))
      expandGlob(table, EvaluationContext("", Path.Root, new DateTime())).flatMap(_.toJson).copoint.toList must_== expected
    }
  }
}

object FSLibSpec extends FSLibSpec[test.YId] with test.YIdInstances
