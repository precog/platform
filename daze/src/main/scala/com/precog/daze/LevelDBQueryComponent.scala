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
import akka.dispatch.Await
import akka.util.duration._
import blueeyes.json.JPath
import blueeyes.util.Clock
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.set._
import Iteratee._

trait LevelDBQueryConfig {
  def clock: Clock
  def projectionRetrievalTimeout: akka.util.Timeout
}

trait LevelDBQueryComponent extends YggConfigComponent with StorageEngineQueryComponent with YggShardComponent with DatasetOpsComponent {
  type YggConfig <: LevelDBQueryConfig
  type Dataset[E] = IterableDataset[E]
  import ops._

  implicit def asyncContext: ExecutionContext

  trait QueryAPI extends StorageEngineQueryAPI[Dataset] {
    type Sources = Set[(JPath, SType, ProjectionDescriptor)]

    /**
     *
     */
    override def fullProjection(userUID: String, path: Path, expiresAt: Long): Dataset[SValue] = Await.result(
      fullProjectionFuture(userUID, path, expiresAt),
      (expiresAt - yggConfig.clock.now().getMillis) millis
    )

    private def fullProjectionFuture(userUID: String, path: Path, expiresAt: Long): Future[Dataset[SValue]] = {
      for {
        pathRoot <- storage.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) 
        dataset  <- assemble(path, sources(JPath.Identity, pathRoot), expiresAt)
      } yield dataset
    }

    /**
     *
     */
    override def mask(userUID: String, path: Path): DatasetMask[Dataset] = LevelDBDatasetMask(userUID, path, None, None) 

    private case class LevelDBDatasetMask(userUID: String, path: Path, selector: Option[JPath], tpe: Option[SType]) extends DatasetMask[Dataset] {
      def derefObject(field: String): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ field })

      def derefArray(index: Int): DatasetMask[Dataset] = copy(selector = selector orElse Some(JPath.Identity) map { _ \ index })

      def typed(tpe: SType): DatasetMask[Dataset] = copy(tpe = Some(tpe))

      def realize(expiresAt: Long): Dataset[SValue] = Await.result(
        (selector, tpe) match {
          case (Some(s), None | Some(SObject) | Some(SArray)) => 
            storage.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot => 
              assemble(path, sources(s, pathRoot), expiresAt)
            }

          case (Some(s), Some(tpe)) => 
            storage.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot =>
              assemble(path, sources(s, pathRoot) filter { case (_, `tpe`, _) => true }, expiresAt)
            }

          case (None   , Some(tpe)) if tpe != SObject && tpe != SArray => 
            storage.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) flatMap { pathRoot =>
              assemble(path, sources(JPath.Identity, pathRoot) filter { case (_, `tpe`, _) => true }, expiresAt)
            }

          case (_      , _        ) => fullProjectionFuture(userUID, path, expiresAt)
        },
        (expiresAt - yggConfig.clock.now().getMillis) millis
      )
    }

    implicit val mergeOrder = Order[Identities].contramap[SColumn] { case (ids, _) => ids }

    def sources(selector: JPath, root: PathRoot): Sources = {
      def search(metadata: PathMetadata, selector: JPath, acc: Set[(JPath, SType, ProjectionDescriptor)]): Sources = {
        metadata match {
          case PathField(name, children) =>
            children.flatMap(search(_, selector \ name, acc))

          case PathIndex(idx, children) =>
            children.flatMap(search(_, selector \ idx, acc))

          case PathValue(valueType, descriptors) => 
            descriptors.headOption map { case (d, _) => acc + ((selector, valueType.stype, d)) } getOrElse acc
        }
      }

      root.children.flatMap(search(_, selector, Set.empty[(JPath, SType, ProjectionDescriptor)]))
    }

    def assemble(path: Path, sources: Sources, expiresAt: Long)(implicit asyncContext: ExecutionContext): Future[Dataset[SValue]] = {
      // pull each projection from the database, then for all the selectors that are provided
      // by tat projection, merge the values
      def retrieveAndJoin(retrievals: Map[ProjectionDescriptor, Set[JPath]]): Future[Dataset[SValue]] = {
        def buildObject(instructions: Set[(JPath, Int)], cvalues: Seq[CValue]) = {
          instructions.foldLeft(SEmptyObject: SValue) {
            case (sv, (selector, columnIndex)) => sv.set(selector, cvalues(columnIndex)).getOrElse(sv)
          }
        }

        def buildInstructions(descriptor: ProjectionDescriptor, selectors: Set[JPath]): Set[(JPath, Int)] = {
          selectors map { s => 
            (s, descriptor.columns.indexWhere(col => col.path == path && s == col.selector)) 
          }
        }

        @tailrec def joinNext(result: Future[Dataset[SValue]], retrievals: List[(ProjectionDescriptor, Set[JPath])]): Future[Dataset[SValue]] = retrievals match {
          case (descriptor, selectors) :: xs => 
            val instr = buildInstructions(descriptor, selectors)

            joinNext(
              for {
                projection <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
                dataset    <- result
              } yield {
                dataset.cogroup(projection.getAllPairs(expiresAt)) {
                  new CogroupF[SValue, Seq[CValue], SValue] {
                    def left(l: SValue) = l
                    def both(l: SValue, r: Seq[CValue]) = l merge buildObject(instr, r)
                    def right(r: Seq[CValue]) = buildObject(instr, r)
                  }
                }
              },
              xs
            )

          case Nil => result
        }

        retrievals.toList match {
          case (descriptor, selectors) :: xs  => 
            val instr = buildInstructions(descriptor, selectors)
            val projection = storage.projection(descriptor, yggConfig.projectionRetrievalTimeout)
            joinNext(projection map { _.getAllPairs(expiresAt) map { buildObject(instr, _) } }, xs)

          case Nil => 
            Future(ops.empty[SValue])
        }
      }

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

      retrieveAndJoin(retrievals)
    }
  }
}
// vim: set ts=4 sw=4 et:
