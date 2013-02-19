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
import scalaz.syntax.copointed._

trait FSLibSpecs[M[+_]] extends Specification with FSLibModule[M] with TestColumnarTableModule[M] { self =>
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

  lazy val projectionMetadata: Map[Path, Set[ColumnRef]] = Map(
    Path("/foo/bar1/baz/quux1")   -> Set(ColumnRef(CPath.Identity, CString)),
    Path("/foo/bar2/baz/quux1")   -> Set(ColumnRef(CPath.Identity, CString)),
    Path("/foo/bar2/baz/quux2")   -> Set(ColumnRef(CPath.Identity, CString)),
    Path("/foo2/bar1/baz/quux1" ) -> Set(ColumnRef(CPath.Identity, CString))
  )                                       
                                          
  def userMetadataView(apiKey: APIKey): StorageMetadata[M] = new StorageMetadata[M] {
    val M = self.M
    def findDirectChildren(path: Path) = M.point(projectionMetadata.keySet.filter(_.isDirectChildOf(path)))
    def findSize(path: Path) = M.point(0L)
    def findSelectors(path: Path) = M.point(projectionMetadata.getOrElse(path, Set.empty[ColumnRef]).map(_.selector))
    def findStructure(path: Path, selector: CPath) = M.point {
      projectionMetadata.getOrElse(path, Set.empty[ColumnRef]).map { structs: Set[ColumnRef] =>
        val types : Map[CType, Long] = structs.collect {
          // FIXME: This should use real counts
          case ColumnRef(selector, ctype) if selector.hasPrefix(selector) => (ctype, 0L)
        }.groupBy(_._1).map { case (tpe, values) => (tpe, values.map(_._2).sum) }

        PathStructure(types, structs.map(_.selector))
      }
    }
  }

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
