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
import metadata._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import scalaz._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec

trait BlockStoreColumnarTableModule[M[+_]] extends ColumnarTableModule[M] with StorageModule[M] {
  type Projection <: BlockProjectionLike[Slice]

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    /** 
     * Determine the set of all projections that could potentially provide columns
     * representing the requested dataset.
     */
    def loadable(metadataView: StorageMetadata[M], path: Path, prefix: JPath, jtpe: JType): M[Set[ProjectionDescriptor]] = {
      jtpe match {
        case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map {
          sources => sources flatMap { source => source.keySet }
        }

        case JArrayFixedT(elements) =>
          (elements map { case (i, jtpe) => loadable(metadataView, path, prefix \ i, jtpe) } toSet).sequence map { _.flatten }

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
          (fields map { case (n, jtpe) => loadable(metadataView, path, prefix \ n, jtpe) } toSet).sequence map { _.flatten }

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
          (Set(loadable(metadataView, path, prefix, tpe1), loadable(metadataView, path, prefix, tpe2))).sequence map { _.flatten }
      }
    }

    /**
     * Find the minimal set of projections (and the relevant columns from each projection) that
     * will be loaded to provide a dataset of the specified type.
     */
    def minimalCover(tpe: JType, descriptors: Set[ProjectionDescriptor]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
      @inline @tailrec
      def cover0(uncovered: Set[ColumnDescriptor], unused: Map[ProjectionDescriptor, Set[ColumnDescriptor]], covers: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
        if (uncovered.isEmpty) {
          covers
        } else {
          val (d0, covered) = unused map { case (d, dcols) => (d, dcols & uncovered) } maxBy { _._2.size } 
          cover0(uncovered &~ covered, unused - d0, covers + (d0 -> covered))
        }
      }

      cover0(
        descriptors.flatMap(_.columns.toSet) filter { cd => includes(tpe, cd.selector, cd.valueType) }, 
        descriptors map { d => (d, d.columns.toSet) } toMap, 
        Map.empty)
    }

    class Cell(val index: Int, var position: Int, var slice: Slice)

    trait CellMatrix { matrix => 
      def compare(cl: Cell, cr: Cell): Ordering

      lazy val toJavaComparator = new java.util.Comparator[Cell] {
        def compare(c1: Cell, c2: Cell) = matrix.compare(c1, c2).toInt
      }
    }

    def cellMatrix(slices: Set[Slice])(keyf: Slice => List[ColumnRef]): CellMatrix = {
      // fill the upper triangle of the matrix, since that is all that will be used
      type ComparatorMatrix = Array[Array[(Int, Int) => Ordering]]
      @inline @tailrec def fillMatrix(l: Vector[(Slice, Int)], comparatorMatrix: ComparatorMatrix): ComparatorMatrix = {
        if (l.isEmpty) comparatorMatrix else {
          for ((s, i) <- l; (s0, i0) <- l.tail) { 
            comparatorMatrix(i)(i0) = Slice.rowComparator(s, s0)(keyf) 
            comparatorMatrix(i0)(i) = Slice.rowComparator(s0, s)(keyf)
          }

          fillMatrix(l.tail, comparatorMatrix)
        }
      }

      new CellMatrix {
        private[this] val comparatorMatrix = fillMatrix(Vector(slices.toSeq: _*).zipWithIndex, Array.ofDim[(Int, Int) => Ordering](slices.size, slices.size))

        def compare(cl: Cell, cr: Cell): Ordering = {
          comparatorMatrix(cl.index)(cr.index)(cl.position, cr.position)
        }
      }
    }

    def load(tpe: JType): M[Table] = {
      val metadataView = storage.userMetadataView(sys.error("TODO"))

      // Reduce this table to obtain the in-memory set of strings representing the vfs paths
      // to be loaded.
      val pathsM = this.reduce {
        new CReducer[Set[Path]] {
          def reduce(columns: JType => Set[Column], range: Range): Set[Path] = {
            columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _ => Set()
            }
          }
        }
      }

      for {
        paths               <- pathsM
        coveringProjections <- (paths map { path => loadable(metadataView, path, JPath.Identity, tpe) }).sequence map { _.flatten }
      } yield {
        val loadableProjections = minimalCover(tpe, coveringProjections)
        table(
          sys.error("todo")
        )
      }
    }
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
