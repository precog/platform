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
package com.precog.yggdrasil
package metadata 

import com.precog.common.json._
import actor._

import com.precog.common._
import com.precog.common.security._

import blueeyes.bkka._
 
import akka.actor._
import akka.pattern.ask
import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import akka.dispatch.MessageDispatcher
import akka.util.Timeout
import akka.util.duration._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

trait StorageMetadata[M[+_]] {
  implicit def M: Monad[M]

  def findChildren(path: Path): M[Set[Path]]
  def findSelectors(path: Path): M[Set[CPath]]
  def findProjections(path: Path, selector: CPath): M[Map[ProjectionDescriptor, ColumnMetadata]]
  def findPathMetadata(path: Path, selector: CPath): M[PathRoot]

  def findProjections(path: Path): M[Map[ProjectionDescriptor, ColumnMetadata]] = {
    findSelectors(path) flatMap { selectors => 
      selectors.traverse(findProjections(path, _)) map { proj =>
        if(proj.size == 0) {
          Map.empty[ProjectionDescriptor, ColumnMetadata]
        } else {
          proj.reduce(_ ++ _) 
        }
      }
    }
  }

  def findProjections(path: Path, selector: CPath, valueType: CType): M[Map[ProjectionDescriptor, ColumnMetadata]] = 
    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }

  def typeFilter(path: Path, selector: CPath, valueType: CType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType == valueType )
  }
}

sealed trait PathMetadata 
case class PathRoot(children: Set[PathMetadata]) 

case class PathField(name: String, children: Set[PathMetadata]) extends PathMetadata
case class PathIndex(idx: Int, children: Set[PathMetadata]) extends PathMetadata
case class PathValue(valueType: CType, authorities: Authorities, descriptors: Map[ProjectionDescriptor, ColumnMetadata]) extends PathMetadata {
  def update(desc: ProjectionDescriptor, meta: ColumnMetadata) = 
    PathValue(valueType, authorities, descriptors + (desc -> meta))
}

class UserMetadataView[M[+_]](uid: String, accessControl: AccessControl[M], metadata: StorageMetadata[M])(implicit val M: Monad[M]) extends StorageMetadata[M] {
  def findChildren(path: Path): M[Set[Path]] = {
    metadata.findChildren(path) flatMap { paths =>
      paths traverse { p =>
        val tPath = path / p
        accessControl.mayAccessPath(uid, tPath, PathRead) map {
          case true => Set(p)
          case false => Set.empty
        }
      } map { _.flatten }
    }
  }

  def findSelectors(path: Path): M[Set[CPath]] = {
    metadata.findSelectors(path) flatMap { selectors =>
      selectors traverse { selector =>
        findProjections(path, selector) map { result =>
          if(result.isEmpty) List.empty else List(selector)
        }
      } map { _.flatten }
    }
  }

  def findProjections(path: Path, selector: CPath): M[Map[ProjectionDescriptor, ColumnMetadata]] = {
    metadata.findProjections(path, selector) flatMap { pmap =>
      traverseFilter(pmap) {
        case (key, value) =>
          traverseForall(value) {
            case (colDesc, _) => 
              val uids = colDesc.authorities.uids
              accessControl.mayAccessData(uid, path, uids, DataQuery)
          }
      }
    }
  }
  
  def findPathMetadata(path: Path, selector: CPath): M[PathRoot] = {
    // TODO: This algorithm can be implemented in a single pass without all this nonsense.
    def restrictAccess(children: Set[PathMetadata]): M[Set[PathMetadata]] = {
      val mapped = children map {
        case PathField(name, children) => 
          restrictAccess(children).map(c => Some(PathField(name, c)))

        case PathIndex(index, children) => 
          restrictAccess(children).map(c => Some(PathIndex(index, c)))

        case p @ PathValue(_, authorities, _) =>
          (accessControl.mayAccessData(uid, path, authorities.uids, DataQuery) map { _ option p })
      }

      mapped.sequence map { _.flatten }
    }

    def removeAllEmpty(children: Set[PathMetadata]): Set[PathMetadata] = {
       children.foldLeft(Set.empty[PathMetadata]){
         case (acc, PathField(name, children)) =>
           val fc = removeAllEmpty(children)
           if (!fc.isEmpty) { acc + PathField(name, fc) } else { acc }

         case (acc, PathIndex(index, children)) => 
           val fc = removeAllEmpty(children)
           if (!fc.isEmpty) { acc + PathIndex(index, fc) } else { acc }

         case (acc, p @ PathValue(_, _, _)) => acc + p
       }
    }

    accessControl.mayAccessPath(uid, path, PathRead).flatMap {
      case true =>
        metadata.findPathMetadata(path, selector).flatMap{ pr => 
          restrictAccess(pr.children) map removeAllEmpty map { PathRoot }
        }
      case false =>
        M.point(PathRoot(Set.empty))
    }
  }

  def traverseFilter[A, B](as: Iterable[(A, B)])(f: ((A, B)) => M[Boolean]): M[Map[A, B]] = {
    for (tx <-  (as map { t => f(t) map { (t, _) } }).toStream.sequence) yield {
      tx collect { case (t, true) => t } toMap
    }
  }

  def traverseForall[A](as: Iterable[A])(f: A => M[Boolean]): M[Boolean] =
    as.map(f).toStream.sequence.map(_ forall identity)
}

class ActorStorageMetadata(actor: ActorRef)(implicit val asyncContext: ExecutionContext) extends StorageMetadata[Future] with Logging {
  implicit val M = AkkaTypeClasses.futureApplicative(asyncContext) 
  implicit val serviceTimeout: Timeout = 10 seconds //TODO: CONFIGURATION!!!
 
  def findChildren(path: Path) = (actor ? FindChildren(path)).mapTo[Set[Path]] onFailure { 
    case e => logger.error("Error finding children for " + path, e) 
  }

  def findSelectors(path: Path) = (actor ? FindSelectors(path)).mapTo[Set[CPath]] onFailure { 
    case e => logger.error("Error finding selectors for " + path, e) 
  }

  def findProjections(path: Path, selector: CPath) = 
    (actor ? FindDescriptors(path, selector)).mapTo[Map[ProjectionDescriptor, ColumnMetadata]] onFailure { 
      case e => logger.error("Error finding projections for " + (path, selector), e) 
    }
  
  def findPathMetadata(path: Path, selector: CPath) = {
    logger.debug("Querying actor for path metadata")
    (actor ? FindPathMetadata(path, selector)).mapTo[PathRoot] onFailure { 
      case e => logger.error("Error finding pathmetadata for " + (path, selector), e) 
    }
  }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } onFailure { case e => logger.error("Error closing ActorStorageMetadata", e) }
}
