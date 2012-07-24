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
package leveldb

import scala.annotation.tailrec

import metadata._
import com.precog.common.Path

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.duration._
import blueeyes.json.JPath
import blueeyes.util.Clock

import scalaz._

abstract class LevelDBProjectionOps[Dataset](clock: Clock, shardMetadata: StorageMetadataSource[Future])(implicit asyncContext: ExecutionContext) {
  type Sources = Set[(JPath, SType, ProjectionDescriptor)]

  /**
   *
   */
  def load(userUID: String, path: Path, expiresAt: Long, release: Release): Dataset = {
    Await.result(
      loadFuture(userUID, path, expiresAt, release),
      (expiresAt - clock.now().getMillis) millis
    )
  }

  private def loadFuture(userUID: String, path: Path, expiresAt: Long, release: Release): Future[Dataset] = {
    for {
      pathRoot <- shardMetadata.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) 
      dataset  <- assemble(path, JPath.Identity, sources(JPath.Identity, pathRoot), expiresAt, release)
    } yield dataset
  }

  /**
   *
   */
  def mask(userUID: String, path: Path): DatasetMask[Dataset] = LevelDBDatasetMask(userUID, path, None, None) 

  private case class LevelDBDatasetMask(userUID: String, path: Path, selector: Option[JPath], tpe: Option[SType]) extends DatasetMask[Dataset] {
    def derefObject(field: String): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ field })

    def derefArray(index: Int): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ index })

    def typed(tpe: SType): DatasetMask[Dataset] = copy(tpe = Some(tpe))

    def realize(expiresAt: Long, release: Release): Dataset = Await.result(
      (selector, tpe) match {
        case (Some(s), None | Some(SObject) | Some(SArray)) => 
          shardMetadata.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot => 
            assemble(path, s, sources(s, pathRoot), expiresAt, release)
          }

        case (Some(s), Some(tpe)) => 
          shardMetadata.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot =>
            assemble(path, s, sources(s, pathRoot) filter { 
              case (_, `tpe`, _) => true
              case _ => false
            }, expiresAt, release)
          }

        case (None   , Some(tpe)) if tpe != SObject && tpe != SArray => 
          shardMetadata.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) flatMap { pathRoot =>
            assemble(path, JPath.Identity, sources(JPath.Identity, pathRoot) filter { 
              case (_, `tpe`, _) => true 
              case _ => false
            }, expiresAt, release)
          }

        case (_      , _        ) => loadFuture(userUID, path, expiresAt, release)
      },
      (expiresAt - clock.now().getMillis) millis
    )
  }

  protected def sources(selector: JPath, root: PathRoot): Sources = {
    def search(metadata: PathMetadata, selector: JPath, acc: Set[(JPath, SType, ProjectionDescriptor)]): Sources = {
      metadata match {
        case PathField(name, children) =>
          children.flatMap(search(_, selector \ name, acc))

        case PathIndex(idx, children) =>
          children.flatMap(search(_, selector \ idx, acc))

        case PathValue(valueType, _, descriptors) => 
          descriptors.headOption map { case (d, _) => acc + ((selector, valueType.stype, d)) } getOrElse acc
      }
    }

    root.children.flatMap(search(_, selector, Set.empty[(JPath, SType, ProjectionDescriptor)]))
  }

  protected def assemble(path: Path, prefix: JPath, sources: Sources, expiresAt: Long, release: Release)(implicit asyncContext: ExecutionContext): Future[Dataset] = {
    // determine the projections from which to retrieve data
    // todo: for right now, this is implemented greedily such that the first
    // projection containing a desired column wins. It should be implemented
    // to choose the projection that satisfies the largest number of columns.
    val minimalDescriptors = sources.foldLeft(Map.empty[JPath, Set[ProjectionDescriptor]]) {
      case (acc, (selector, _, descriptor)) => 
        acc.get(selector) match {
          case Some(chosen) if chosen.contains(descriptor) ||
                               (chosen exists { d => descriptor.columnAt(path, selector).exists(d.satisfies) }) => acc

          case _ => acc + (selector -> (acc.getOrElse(selector, Set.empty[ProjectionDescriptor]) + descriptor))
        }
    }

    val retrievals = minimalDescriptors.foldLeft(Map.empty[ProjectionDescriptor, Set[JPath]]) {
      case (acc, (jpath, descriptors)) => descriptors.foldLeft(acc) {
        (acc, descriptor) => acc + (descriptor -> (acc.getOrElse(descriptor, Set.empty[JPath]) + jpath))
      }
    }

    retrieveAndJoin(path, prefix, retrievals, expiresAt, release)
  }

  protected def retrieveAndJoin(path: Path, prefix: JPath, retrievals: Map[ProjectionDescriptor, Set[JPath]], expiresAt: Long, release: Release): Future[Dataset]
}
// vim: set ts=4 sw=4 et:
