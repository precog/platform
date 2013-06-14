package com.precog.yggdrasil
package vfs

import actor._
import execution._
import metadata._
import util._
import SValue._
import ResourceError._
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
import scalaz.syntax.std.option._

import scala.collection.immutable.SortedMap
import scala.collection.immutable.TreeMap

class StubVFSMetadata[M[+_]](projectionMetadata: Map[Path, Map[ColumnRef, Long]])(implicit M: Monad[M]) extends VFSMetadata[M]{
  def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[PathMetadata]] = EitherT.right {
    import PathMetadata._
    M point {
      projectionMetadata.keySet collect {
        case key if key.isChildOf(path) => 
          PathMetadata(
            Path(key.components(path.length)), 
            if (key.length == path.length + 1) DataOnly(FileContent.XQuirrelData) else PathOnly
          )
      }
    }
  }

  private def getPathMeta(path: Path): EitherT[M, ResourceError, Map[ColumnRef, Long]] = EitherT {
    M.point(projectionMetadata.get(path) \/> NotFound("No metadata found for path %s".format(path.path)))
  }

  def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = {
    for {
      types <- getPathMeta(path) map { _ collect {
        case (ColumnRef(`property`, ctype), count) => (ctype, count)
      } }

      children <- getPathMeta(path) map { _ flatMap {
        case t @ (ColumnRef(s, ctype), count) =>
          if (s.hasPrefix(property)) s.take(property.length + 1) else None
      } }
    } yield PathStructure(types, children.toSet)
  }

  def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long] = {
    getPathMeta(path) map(_.values.max)
  }
}
