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

import com.precog.common.Path
import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.leveldb._

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
  type X = Throwable

  def storage: Storage
  implicit def asyncContext: ExecutionContext

  type Dataset[E] = DatasetEnum[X, E, IO]

  trait QueryAPI extends StorageEngineQueryAPI[Dataset] {
    /*
    override def fullProjection(userUID: String, path: Path, expiresAt: Long): DatasetEnum[X, SEvent, IO] = DatasetEnum(
      for {
        selectors   <- storage.userMetadataView(userUID).findSelectors(path) 
        sources     <- Future.sequence(selectors map { s => storage.userMetadataView(userUID).findProjections(path, s) map { p => (s, p.keySet) } })
        enumerator: EnumeratorP[X, Vector[SEvent], IO]  <- assemble(path, sources, expiresAt)
      } yield enumerator
    )

    override def mask(userUID: String, path: Path): DatasetMask[Dataset] = LevelDBDatasetMask(userUID, path, None, None) 

    private case class LevelDBDatasetMask(userUID: String, path: Path, selector: Option[JPath], tpe: Option[SType]) extends DatasetMask[Dataset] {
      def derefObject(field: String): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ field })

      def derefArray(index: Int): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ index })

      def typed(tpe: SType): DatasetMask[Dataset] = copy(tpe = Some(tpe))

      def realize(expiresAt: Long)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
        def assembleForSelector(selector: JPath, retrieval: Future[Map[ProjectionDescriptor, ColumnMetadata]]) = 
          DatasetEnum(
            for {
              descriptors <- retrieval 
              // we don't know whether the descriptors retuned contain exactly the selector requested, 
              // or children as well so we have to reassemble them such that we have relevant projections
              // grouped by unique selector that is a child of the specified selector
              val sources = descriptors flatMap {
                case (descriptor, _) => 
                  val baseSources = descriptor.columns collect { 
                    case c if c.selector.nodes startsWith selector.nodes => (c.selector, Set(descriptor)) 
                  } 
                  
                  baseSources.foldLeft(Map.empty[JPath, Set[ProjectionDescriptor]]) {
                    case (acc, (s, ds)) => acc + (s -> (acc.getOrElse(s, Set()) ++ ds))
                  }
              }
              enum        <- assemble(path, sources.toSeq, expiresAt) 
            } yield {
              enum map { _ map { case (ids, sv) => (sv \ selector) map { v => (ids, v) } } flatten }
            }
          )

        (selector, tpe) match {
          case (Some(s), None | Some(SObject) | Some(SArray)) => 
            assembleForSelector(s, storage.userMetadataView(userUID).findProjections(path, s))

          case (Some(s), Some(tpe)) => 
            assembleForSelector(s, storage.userMetadataView(userUID).findProjections(path, s, tpe))

          case (None   , Some(tpe)) if tpe != SObject && tpe != SArray => 
            assembleForSelector(JPath.Identity, storage.userMetadataView(userUID).findProjections(path, JPath.Identity))

          case (_      , _        ) => fullProjection(userUID, path, expiresAt)
        }
      }
    }

    implicit val mergeOrder = Order[Identities].contramap[SColumn] { case (ids, _) => ids }

    def assemble(path: Path, sources: Seq[(JPath, Set[ProjectionDescriptor])], expiresAt: Long)(implicit asyncContext: ExecutionContext): Future[EnumeratorP[X, Vector[SEvent], IO]] = {
      def retrieveAndMerge(path: Path, selector: JPath, descriptors: Set[ProjectionDescriptor]): Future[EnumeratorP[X, Vector[SColumn], IO]] = {
        import scalaz.std.list._
        {for {
          projections <- Future.sequence(descriptors map { storage.projection(_, yggConfig.projectionRetrievalTimeout) })
        } yield {
          mergeAllChunked(projections.map(_.getColumnValues(path, selector, expiresAt)).toSeq: _*)
        }} recover {
          case ex => EnumeratorP.pointErr(new RuntimeException("An error occurred retrieving data for " + descriptors, ex))
        }
      }

      // determine the projections from which to retrieve data
      // todo: for right now, this is implemented greedily such that the first
      // projection containing a desired column wins. It should be implemented
      // to choose the projection that satisfies the largest number of columns.
      val descriptors = sources.foldLeft(Map.empty[JPath, Set[ProjectionDescriptor]]) {
        case (acc, (selector, descriptorData)) => descriptorData.foldLeft(acc) {
          case (acc, descriptor) => 
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

      Future.sequence(mergedFutures) map { en => combine(en.toList) }
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
    */
  }
}
// vim: set ts=4 sw=4 et:
