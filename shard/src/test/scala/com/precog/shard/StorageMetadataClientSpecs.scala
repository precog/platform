package com.precog.shard

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.metadata._

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._

import org.specs2.mutable._

abstract class StorageMetadataClientSpecs[M[+_]](implicit val M: Monad[M] with Copointed[M]) extends Specification {
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

  val client = new StorageMetadataClient(new StorageMetadataSource[M] {
    def userMetadataView(userUID: String) = new StubStorageMetadata(projectionMetadata)
  })

  "browse" should {
    "find child paths" in {
      client.browse("", Path("/foo/")).copoint must beLike {
        case Success(JArray(results)) => results must haveTheSameElementsAs(JString("/bar/") :: JString("/bar1/") :: JString("/bar2/") :: Nil)
      }
    }
  }

  "structure" should {
    "find correct node information" in {
      client.structure("", Path("/foo/bar"), CPath.Identity).copoint must beLike {
        case Success(result) => result must_== JObject("children" -> JArray(JString(".bar") :: JString(".baz") :: Nil), "types" -> JObject())
      }
    }

    "find correct leaf types" in {
      client.structure("", Path("/foo/bar"), CPath("bar")).copoint must beLike {
        case Success(result) => result must_== JObject("children" -> JArray(), "types" -> JObject("Long" -> JNum(50)))
      }
    }
  }
}

object StorageMetadataClientSpecs extends StorageMetadataClientSpecs[Need]

// vim: set ts=4 sw=4 et:
