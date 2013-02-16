package com.precog.shard

import com.precog.common._
import com.precog.common.json._
import com.precog.yggdrasil._
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

  val fbbar = ColumnRef(Path("/foo/bar/"), CPath(".bar"), CString, Authorities(Set()))
  val fbbaz = ColumnRef(Path("/foo/bar/"), CPath(".baz"), CString, Authorities(Set()))

  val projectionMetadata: Map[ProjectionDescriptor, ColumnMetadata] = Map(
    ProjectionDescriptor(1, ColumnRef(Path("/foo/bar1/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnRef(Path("/foo/bar2/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnRef(Path("/foo/bar2/baz/quux2"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, ColumnRef(Path("/foo2/bar1/baz/quux1"), CPath(), CString, Authorities(Set())) :: Nil) -> ColumnMetadata.Empty,
    ProjectionDescriptor(1, fbbar :: Nil) -> colSizeMetadata(fbbar, 123L),
    ProjectionDescriptor(1, fbbaz :: Nil) -> colSizeMetadata(fbbaz, 456L)
  )

  val client = new StorageMetadataClient(new StorageMetadataSource[M] {
    def userMetadataView(userUID: String) = new StubStorageMetadata[M](projectionMetadata)
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
        case Success(result) => result must_== JObject("children" -> JArray(), "types" -> JObject("String" -> JNum(123L)))
      }
    }
  }
}

object StorageMetadataClientSpecs extends StorageMetadataClientSpecs[Need]

// vim: set ts=4 sw=4 et:
