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

import actor._

import com.precog.common._
import com.precog.common.security._

import blueeyes.json.JPath
 
import akka.actor._
import akka.pattern.ask
import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Timeout
import akka.util.duration._

trait StorageMetadata {

  implicit val dispatcher: MessageDispatcher

  def findSelectors(path: Path): Future[Seq[JPath]]

  def findProjections(path: Path): Future[Map[ProjectionDescriptor, ColumnMetadata]] = {
    findSelectors(path).flatMap { selectors => 
      Future.traverse( selectors )( findProjections(path, _) ) map { proj =>
        if(proj.size == 0) {
          Map.empty[ProjectionDescriptor, ColumnMetadata]
        } else {
          proj.reduce(_ ++ _) }
        }
    }
  }

  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]]

  def findProjections(path: Path, selector: JPath, valueType: SType): Future[Map[ProjectionDescriptor, ColumnMetadata]] = 
    findProjections(path, selector) map { m => m.filter(typeFilter(path, selector, valueType) _ ) }

  def typeFilter(path: Path, selector: JPath, valueType: SType)(t: (ProjectionDescriptor, ColumnMetadata)): Boolean = {
    t._1.columns.exists( col => col.path == path && col.selector == selector && col.valueType =~ valueType )
  }
}

trait MetadataView extends StorageMetadata

class IdentityMetadataView(metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView {
  def findSelectors(path: Path) = metadata.findSelectors(path)
  def findProjections(path: Path, selector: JPath) = metadata.findProjections(path, selector)
}

class UserMetadataView(uid: String, accessControl: AccessControl, metadata: StorageMetadata)(implicit val dispatcher: MessageDispatcher) extends MetadataView { 
  
  def findSelectors(path: Path): Future[Seq[JPath]] = {
    metadata.findSelectors(path) flatMap { selectors =>
      Future.traverse(selectors) { selector =>
        findProjections(path, selector) map { result =>
          if(result.isEmpty) List.empty else List(selector)
        }
      } map { _.flatten }
    }
  }

  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]] = {
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

  def traverseFilter[A, B](as: Traversable[(A, B)])(f: ((A, B)) => Future[Boolean]): Future[Map[A, B]] = {
    val tx = as.map( t => f(t) map { (t, _) } )
    Future.fold(tx)(Map.empty[A,B]){
      case (acc, (t, b)) => if(b) { acc + t } else { acc }
    }
  }

  def traverseForall[A](as: Traversable[A])(f: A => Future[Boolean]): Future[Boolean] =
    if(as.size == 0) {
      Future(true)
    } else {
      Future.reduce(as.map(f))(_ && _)
    }
}

class ShardMetadata(actor: ActorRef)(implicit val dispatcher: MessageDispatcher) extends StorageMetadata {

  implicit val serviceTimeout: Timeout = 10 seconds
 
  def findSelectors(path: Path) = actor ? FindSelectors(path) map { _.asInstanceOf[Seq[JPath]] }

  def findProjections(path: Path, selector: JPath) = 
    actor ? FindDescriptors(path, selector) map { _.asInstanceOf[Map[ProjectionDescriptor, ColumnMetadata]] }

  def close(): Future[Unit] = actor ? PoisonPill map { _ => () } 

}

