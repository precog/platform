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

  "size" should {
    "find correct size for single-column path" in {
      client.size("", Path("/foo/bar1/baz/quux1")).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case JNum(result) => result mustEqual 10
      }
    }

    "find correct size for multi-column path" in {
      client.size("", Path("/foo/bar")).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case JNum(result) => result mustEqual 60
      }
    }

    "find default (0) size for non-existent path" in {
      client.size("", Path("/not/really")).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case JNum(result) => result mustEqual 0
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

    "find default empty result for non-existent path" in {
      client.structure("", Path("/bar/foo"), CPath.Identity).valueOr(e => sys.error(e.toString)).copoint must beLike {
        case result => result must_== JObject()
      }
    }
  }
}

object BrowseServiceSpecs extends BrowseServiceSpecs[Need]

// vim: set ts=4 sw=4 et:
