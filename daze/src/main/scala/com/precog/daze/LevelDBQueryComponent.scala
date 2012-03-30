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
import blueeyes.json.JPathField
import blueeyes.json.JPathIndex
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
  type Dataset[E] <: IterableDataset[E]
  import ops._

  implicit def asyncContext: ExecutionContext

  trait QueryAPI extends StorageEngineQueryAPI[Dataset] {
    type Sources = Set[(JPath, SType, ProjectionDescriptor)]

    /**
     *
     */
    override def fullProjection(userUID: String, path: Path, expiresAt: Long): Dataset[SValue] = {
      Await.result(
        fullProjectionFuture(userUID, path, expiresAt),
        (expiresAt - yggConfig.clock.now().getMillis) millis
      )
    }

    private def fullProjectionFuture(userUID: String, path: Path, expiresAt: Long): Future[Dataset[SValue]] = {
      for {
        pathRoot <- storage.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) 
        dataset  <- assemble(path, JPath.Identity, sources(JPath.Identity, pathRoot), expiresAt)
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
              assemble(path, s, sources(s, pathRoot), expiresAt)
            }

          case (Some(s), Some(tpe)) => 
            storage.userMetadataView(userUID).findPathMetadata(path, s) flatMap { pathRoot =>
              assemble(path, s, sources(s, pathRoot) filter { 
                case (_, `tpe`, _) => true
                case _ => false
              }, expiresAt)
            }

          case (None   , Some(tpe)) if tpe != SObject && tpe != SArray => 
            storage.userMetadataView(userUID).findPathMetadata(path, JPath.Identity) flatMap { pathRoot =>
              assemble(path, JPath.Identity, sources(JPath.Identity, pathRoot) filter { 
                case (_, `tpe`, _) => true 
                case _ => false
              }, expiresAt)
            }

          case (_      , _        ) => fullProjectionFuture(userUID, path, expiresAt)
        },
        (expiresAt - yggConfig.clock.now().getMillis) millis
      )
    }

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

    def assemble(path: Path, prefix: JPath, sources: Sources, expiresAt: Long)(implicit asyncContext: ExecutionContext): Future[Dataset[SValue]] = {
      // pull each projection from the database, then for all the selectors that are provided
      // by tat projection, merge the values
      def retrieveAndJoin(retrievals: Map[ProjectionDescriptor, Set[JPath]]): Future[Dataset[SValue]] = {
        def appendToObject(sv: SValue, instructions: Set[(JPath, Int)], cvalues: Seq[CValue]) = {
          instructions.foldLeft(sv) {
            case (sv, (selector, columnIndex)) => sv.set(selector, cvalues(columnIndex)).getOrElse(sv)
          }
        }

        def buildInstructions(descriptor: ProjectionDescriptor, selectors: Set[JPath]): (SValue, Set[(JPath, Int)]) = {
          Tuple2(
            selectors.flatMap(_.dropPrefix(prefix).flatMap(_.head)).toList match {
              case List(JPathField(_)) => SObject.Empty
              case List(JPathIndex(_)) => SArray.Empty
              case Nil => SNull
              case _ => sys.error("Inconsistent JSON structure: " + selectors)
            },
            selectors map { s =>
              (s.dropPrefix(prefix).get, descriptor.columns.indexWhere(col => col.path == path && s == col.selector)) 
            }
          )
        }

        def joinNext(retrievals: List[(ProjectionDescriptor, Set[JPath])]): Future[Dataset[SValue]] = retrievals match {
          case (descriptor, selectors) :: x :: xs => 
            val (init, instr) = buildInstructions(descriptor, selectors)
            for {
              projection <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
              dataset    <- joinNext(x :: xs)
            } yield {
              val result = projection.getAllPairs(expiresAt).cogroup(dataset) {
                new CogroupF[Seq[CValue], SValue, SValue] {
                  def left(l: Seq[CValue]) = appendToObject(init, instr, l)
                  def both(l: Seq[CValue], r: SValue) = appendToObject(r, instr, l)
                  def right(r: SValue) = r
                }
              }
              result
            }

          case (descriptor, selectors) :: Nil =>
            val (init, instr) = buildInstructions(descriptor, selectors)
            for {
              projection <- storage.projection(descriptor, yggConfig.projectionRetrievalTimeout) 
            } yield {
              val result = ops.extend(projection.getAllPairs(expiresAt)) map { appendToObject(init, instr, _) }
              result
            }
        }

        if (retrievals.isEmpty) Future(ops.empty[SValue](1)) else joinNext(retrievals.toList)
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
