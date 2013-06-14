package com.precog.daze

import com.precog.common._
import com.precog.bytecode._
import com.precog.common.Path
import com.precog.common.accounts._

import com.precog.common.security._
import com.precog.yggdrasil._
import com.precog.yggdrasil.execution.EvaluationContext
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.vfs._

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
                                          
  val vfs = new StubVFSMetadata[M](projectionMetadata)

  def pathTable(path: String) = {
    Table.constString(Set(path)).transform(WrapObject(Leaf(Source), TransSpecModule.paths.Value.name))
  }

  val testAPIKey = "testAPIKey"
  def testAccount = AccountDetails("00001", "test@email.com",
    new DateTime, "testAPIKey", Path.Root, AccountPlan.Free)
  val defaultEvaluationContext = EvaluationContext(testAPIKey, testAccount, Path.Root, Path.Root, new DateTime)
  val defaultMorphContext = MorphContext(defaultEvaluationContext, new MorphLogger {
    def info(msg: String): M[Unit] = M.point(())
    def warn(msg: String): M[Unit] = M.point(())
    def error(msg: String): M[Unit] = M.point(())
    def die(): M[Unit] = M.point(sys.error("MorphContext#die()"))
  })

  def runExpansion(table: Table): List[JValue] = {
    expandGlob(table, defaultMorphContext).map(_.transform(SourceValue.Single)).flatMap(_.toJson).copoint.toList
  }

  "path globbing" should {
    "not alter un-globbed paths" in {
      val table = pathTable("/foo/bar/baz/")
      val expected: List[JValue] = List(JString("/foo/bar/baz/"))
      runExpansion(table) must_== expected
    }
    
    "not alter un-globbed relative paths" in {
      val table = pathTable("foo")
      val expected: List[JValue] = List(JString("/foo/"))
      runExpansion(table) mustEqual expected
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
