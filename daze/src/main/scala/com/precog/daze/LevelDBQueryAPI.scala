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
package com.precog
package daze

import scala.annotation.tailrec

import com.precog.yggdrasil._
import com.precog.yggdrasil.kafka._
import com.precog.yggdrasil.leveldb._
import com.precog.yggdrasil.shard._
import com.precog.analytics.Path

import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.duration._
import blueeyes.json.JPath
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.set._
import Iteratee._

trait LevelDBQueryConfig {
  def projectionRetrievalTimeout: akka.util.Timeout
}

trait LevelDBQueryComponent extends YggConfigComponent with StorageEngineQueryComponent {
  type YggConfig <: LevelDBQueryConfig
  type Storage <: YggShard

  def storage: Storage

  trait QueryAPI extends StorageEngineQueryAPI {
    override def fullProjection[X](userUID: String, path: Path)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = DatasetEnum(
      for {
        selectors   <- storage.userMetadataView(userUID).findSelectors(path) 
        sources     <- Future.sequence(selectors.map(s => storage.userMetadataView(userUID).findProjections(path, s).map(p => (s, p))))
        enumerator: EnumeratorP[X, Vector[SEvent], IO]  <- assemble(path, sources)
      } yield enumerator
    )

    override def mask[X](userUID: String, path: Path): DatasetMask[X] = LevelDBDatasetMask[X](userUID, path, None, None) 

    private case class LevelDBDatasetMask[X](userUID: String, path: Path, selector: Option[JPath], tpe: Option[SType]) extends DatasetMask[X] {
      def derefObject(field: String): DatasetMask[X] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ field })

      def derefArray(index: Int): DatasetMask[X] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ index })

      def typed(tpe: SType): DatasetMask[X] = copy(tpe = Some(tpe))

      def realize(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
        def assembleForSelector(selector: JPath, retrieval: Future[Map[ProjectionDescriptor, ColumnMetadata]]) = 
          DatasetEnum(
            for {
              descriptors <- retrieval 
              enum        <- assemble[X](path, List((selector, descriptors))) 
            } yield {
              enum map { _ map { case (ids, sv) => (sv \ selector) map { v => (ids, v) } } collect { case Some(t) => t } }
            }
          )

        (selector, tpe) match {
          case (Some(s), None | Some(SObject) | Some(SArray)) => assembleForSelector(s, storage.userMetadataView(userUID).findProjections(path, s))
          case (Some(s), Some(tpe)) => assembleForSelector(s, storage.userMetadataView(userUID).findProjections(path, s, tpe))
          case (None   , Some(tpe)) if tpe != SObject && tpe != SArray => assembleForSelector(JPath.Identity, storage.userMetadataView(userUID).findProjections(path, JPath.Identity))
          case (_      , _        ) => fullProjection(userUID, path)
        }
      }
    }

    implicit val mergeOrder = Order[Identities].contramap((scol: SColumn) => scol._1)

    def assemble[X](path: Path, sources: Seq[(JPath, Map[ProjectionDescriptor, ColumnMetadata])])(implicit asyncContext: ExecutionContext): Future[EnumeratorP[X, Vector[SEvent], IO]] = {
      def retrieveAndMerge[X](path: Path, selector: JPath, descriptors: Set[ProjectionDescriptor]): Future[EnumeratorP[X, Vector[SColumn], IO]] = {
        import scalaz.std.list._
        for {
          projections <- Future.sequence(descriptors map { storage.projection(_)(yggConfig.projectionRetrievalTimeout) })
        } yield {
          mergeAllChunked(projections.map(_.getColumnValues[X](path, selector)).toSeq: _*)
        }
      }

      // determine the projections from which to retrieve data
      // todo: for right now, this is implemented greedily such that the first
      // projection containing a desired column wins. It should be implemented
      // to choose the projection that satisfies the largest number of columns.
      val descriptors = sources.foldLeft(Map.empty[JPath, Set[ProjectionDescriptor]]) {
        case (acc, (selector, descriptorData)) => descriptorData.foldLeft(acc) {
          case (acc, (descriptor, _)) => 
            acc.get(selector) match {
              case Some(chosen) if chosen.contains(descriptor) ||
                                   (chosen exists { d => descriptor.columnAt(path, selector).exists(d.satisfies) }) => acc

              case _ => acc + (selector -> (acc.getOrElse(selector, Set.empty[ProjectionDescriptor]) + descriptor))
            }
        }
      }

      // now use merge with identity ordering to produce an enumerator for each selector. Given identity ordering, 
      // encountering the middle case is an error since no single identity should ever correspond to 
      // two values of different types, so the resulting merged set should not contain any duplicate identities.
      val mergedFutures = descriptors map {
        case (selector, descriptors) => retrieveAndMerge(path, selector, descriptors).map((e: EnumeratorP[X, Vector[(Identities, CValue)], IO]) => (selector, e))
      }

      implicit val SEventOrder = SEventIdentityOrder

      Future.sequence(mergedFutures) map { en => combine[X](en.toList) }
    }

    def combine[X](enumerators: List[(JPath, EnumeratorP[X, Vector[SColumn], IO])])(implicit o: Order[SEvent]): EnumeratorP[X, Vector[SEvent], IO] = {
      innerCombine(enumerators)
    }

    private def innerCombine[X](enumerators: List[(JPath, EnumeratorP[X, Vector[SColumn], IO])])(implicit ord: Order[SEvent]): EnumeratorP[X, Vector[SEvent], IO] = {
        enumerators match {
          case (selector, column) :: xs => 
            cogroupEChunked[X, SEvent, SColumn, IO].apply(innerCombine(xs), column).map { _ map {
              case Left3(sevent) => sevent
              case Middle3(((id, svalue), (_, cv))) => (id, svalue.set(selector, cv).getOrElse(sys.error("Cannot reassemble object: conflicting values for " + selector)))
              case Right3((id, cv)) => (id, SValue(selector, cv))
            } }
          case Nil => EnumeratorP.empty[X, Vector[SEvent], IO]
        }
      }

  }
}
// vim: set ts=4 sw=4 et:
