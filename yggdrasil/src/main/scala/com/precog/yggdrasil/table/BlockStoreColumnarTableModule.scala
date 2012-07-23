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
package table

import com.precog.common.Path
import com.precog.bytecode._
import Schema._

import akka.dispatch.{ExecutionContext,Future}

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scalaz._
import scalaz.std.set._
import scalaz.syntax.monoid._


trait BlockStoreColumnarTableModule extends ColumnarTableModule with StorageModule {
  type Projection <: BlockProjectionLike[Slice]

  class Table(slices: StreamT[Future, Slice]) extends ColumnarTable(slices) {
    def load(tpe: JType): Future[Table] = {
      val pathsFuture: Future[Set[String]] = reduce {
        new CReducer[Set[String]] {
          def reduce(columns: JType => Set[Column], range: Range): Set[String] = {
            columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(s)
              case _ => Set()
            }
          }
        }
      }

      val metadataView = storage.userMetadataView(sys.error("TODO"))

      def loadable(path: Path, prefix: JPath, jtpe: JType): Future[Set[ProjectionDescriptor]] = {
        tpe match {
          case p: JPrimitiveType => Future.sequence(ctypes(p).map(metadataView.findProjections(path, prefix, _))) map {
            sources => sources flatMap { source => source.keySet }
          }

          case JArrayFixedT(elements) =>
            Future.sequence(elements map { case (i, jtpe) => loadable(path, prefix \ i, jtpe) }) map { _.flatten.toSet }

          case JArrayUnfixedT =>
            metadataView.findProjections(path, prefix) map { 
              _.keySet filter { 
                _.columns exists { 
                  case ColumnDescriptor(`path`, selector, _, _) => 
                    (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[JPathIndex])
                }
              }
            }

          case JObjectFixedT(fields) =>
            Future.sequence(fields map { case (n, jtpe) => loadable(path, prefix \ n, jtpe) }) map { _.flatten.toSet }

          case JObjectUnfixedT =>
            metadataView.findProjections(path, prefix) map { 
              _.keySet filter { 
                _.columns exists { 
                  case ColumnDescriptor(`path`, selector, _, _) => 
                    (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[JPathField])
                }
              }
            }

          case JUnionT(tpe1, tpe2) =>
            Future.sequence(Set(loadable(path, prefix, tpe1), loadable(path, prefix, tpe2))) map { _.flatten }
        }
      }

      def minimalCover(descriptors: Set[ProjectionDescriptor]): Set[ProjectionDescriptor] = sys.error("margin to small")

      def coveringSchema(descriptors: Set[ProjectionDescriptor]): Seq[(JPath, CType)] = sys.error("todo")

      for {
        paths               <- pathsFuture
        coveringProjections <- Future.sequence(paths map { path => loadable(Path(path), JPath.Identity, tpe) }) map { _.flatten }
                               if (subsumes(coveringSchema(coveringProjections), tpe))
      } yield {
        val loadableProjections = minimalCover(coveringProjections)
        table(
          sys.error("todo")
        )
      }
    }
  }

  def table(slices: StreamT[Future, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
