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
package nihdb

import com.precog.common._
import com.precog.common.security.APIKey
import com.precog.yggdrasil.metadata.{PathStructure, StorageMetadata}

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Future, Promise}
import akka.pattern.ask
import akka.util.Timeout

import com.precog.niflheim._

import blueeyes.bkka.FutureMonad

import scalaz._
import scalaz.syntax.monad._

class NIHDBStorageMetadata(apiKey: APIKey, projectionsActor: ActorRef, actorSystem: ActorSystem, storageTimeout: Timeout) extends StorageMetadata[Future] {
  implicit val asyncContext = actorSystem.dispatcher
  implicit val timeout = storageTimeout

  implicit def M: Monad[Future] = new FutureMonad(actorSystem.dispatcher)

  def findDirectChildren(path: Path): Future[Set[Path]] = {
    val paths = (projectionsActor ? FindChildren(path)).mapTo[Set[Path]]
    paths.map(_.flatMap(_ - path))
  }

  private def findProjection(path: Path): Future[Option[NIHDBProjection]] =
    (projectionsActor ? AccessProjection(path, apiKey)).mapTo[Option[NIHDBProjection]]

  def findSize(path: Path): Future[Long] = findProjection(path).flatMap {
    case Some(proj) => proj.length
    case None => Promise.successful(0L)(asyncContext)
  }

  def findSelectors(path: Path): Future[Set[CPath]] = findProjection(path).flatMap {
    case Some(proj) => proj.structure.map(_.map(_.selector))
    case None => Promise.successful(Set.empty[CPath])(asyncContext)
  }

  def findStructure(path: Path, selector: CPath): Future[PathStructure] = {
    OptionT(findProjection(path)) flatMapF (_.getSnapshot()) flatMapF { snapshot =>
      val childrenM = M.point { snapshot.structure map (_._1) }
      // val countM = M.point { snapshot.count(Some(Set(selector))) }
      val typesM = M.point { snapshot.reduce(Reductions.count, selector) }
      (childrenM /*|@| countM*/ |@| typesM) { (children/*, count*/, types) =>
        PathStructure(types, children)
      }
    } getOrElse PathStructure.Empty
  }
}

trait NIHDBStorageMetadataSource extends StorageMetadataSource[Future] {
  val projectionsActor: ActorRef
  val actorSystem: ActorSystem
  val storageTimeout: Timeout

  def userMetadataView(apiKey: APIKey): StorageMetadata[Future] = 
    new NIHDBStorageMetadata(apiKey, projectionsActor, actorSystem, storageTimeout)
}
