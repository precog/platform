package com.precog.yggdrasil

import actor._
import metadata._
import util._
import SValue._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.util._

import com.precog.util._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.testkit.TestActorRef
import akka.util.Timeout
import akka.util.duration._

import blueeyes.json._

import scalaz._
import scalaz.effect._
import scalaz.syntax.std.boolean._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class StubStorageMetadata[M[+_]](projectionMetadata: Map[Path, Map[ColumnRef, Long]])(implicit M: Monad[M]) extends StorageMetadata[M]{
  def findDirectChildren(path: Path) = M point {
    projectionMetadata.keySet collect {
      case key if key.isChildOf(path) => Path(key.components(path.length))
    }
  }

  def findSize(path: Path) = M.point(0L)
  def findSelectors(path: Path) = M.point(projectionMetadata.getOrElse(path, Map.empty[ColumnRef, Long]).keySet.map(_.selector))
  def findStructure(path: Path, selector: CPath) = M.point {
    val types: Map[CType, Long] = projectionMetadata.getOrElse(path, Map.empty[ColumnRef, Long]) collect {
      case (ColumnRef(`selector`, ctype), count) => (ctype, count)
    }

    val children = projectionMetadata.getOrElse(path, Map.empty[ColumnRef, Long]) flatMap {
      case t @ (ColumnRef(s, ctype), count) => 
        if (s.hasPrefix(selector)) s.take(selector.length + 1) else None
    }

    PathStructure(types, children.toSet)
  }

  def currentVersion(path: Path) = M.point(None)
  def currentAuthorities(path: Path) = M.point(None)
}

trait StubProjectionModule[M[+_], Key, Block] extends ProjectionModule[M, Key, Block] { self =>
  implicit def M: Monad[M]

  protected def projections: Map[Path, Projection]

  class ProjectionCompanion extends ProjectionCompanionLike[M] {
    def apply(path: Path) = M.point(projections.get(path))
  }
}
