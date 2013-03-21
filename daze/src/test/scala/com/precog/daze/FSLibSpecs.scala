package com.precog.daze

import com.precog.common._
import com.precog.bytecode._
import com.precog.common.Path
import com.precog.common.json._
import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import org.joda.time.DateTime
import org.specs2.mutable.Specification

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.comonad._

trait FSLibSpecs[M[+_]] extends Specification with FSLibModule[M] with TestColumnarTableModule[M] { self =>
  import trans._
  import constants._

  val library = new FSLib {}
  import library._

  class YggConfig extends IdSourceConfig with ColumnarTableModuleConfig {
    val maxSliceSize = 10
    val smallSliceSize = 3
    val idSource = new FreshAtomicIdSource
  }

  lazy val yggConfig = new YggConfig

  lazy val projectionMetadata: Map[Path, Map[ColumnRef, Long]] = Map(
    Path("/foo/bar1/baz/quux1")   -> Map(ColumnRef(CPath.Identity, CString) -> 10L),
    Path("/foo/bar2/baz/quux1")   -> Map(ColumnRef(CPath.Identity, CString) -> 20L),
    Path("/foo/bar2/baz/quux2")   -> Map(ColumnRef(CPath.Identity, CString) -> 30L),
    Path("/foo2/bar1/baz/quux1" ) -> Map(ColumnRef(CPath.Identity, CString) -> 40L)
  )                                       
                                          
  def userMetadataView(apiKey: APIKey): StorageMetadata[M] = new StubStorageMetadata[M](projectionMetadata)

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
