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
    val idSource = new FreshAtomicIdSource
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
