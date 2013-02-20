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

import com.precog.common._
import com.precog.common.security._
import com.precog.bytecode._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.util._
import com.precog.util._
import Schema._
import metadata._

import com.precog.util.{BitSet, BitSetUtil, Loop}
import com.precog.util.BitSetUtil.Implicits._

import java.io.File
import java.util.SortedMap
import java.util.Comparator

import org.apache.jdbm.DBMaker
import org.apache.jdbm.DB

import org.slf4j.LoggerFactory

import scalaz._
import scalaz.Ordering._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

import TableModule._

trait SliceColumnarTableModule[M[+_], Key] extends BlockStoreColumnarTableModule[M] with ProjectionModule[M, Key, Slice] with StorageMetadataSource[M] {
  type TableCompanion <: SliceColumnarTableCompanion

  trait SliceColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    type BD = BlockProjectionData[Key, Slice]
  
    private object loadMergeEngine extends MergeEngine[Key, BD]

    def load(table: Table, apiKey: APIKey, tpe: JType): M[Table] = {
      for {
        paths       <- pathsM(table)
        projections <- paths.map(Projection(_)).sequence.map(_.flatten)
        totalLength = projections.map(_.length).sum
      } yield {
        def slices(proj: Projection, constraints: Option[Set[ColumnRef]]): StreamT[M, Slice] = {
          StreamT.unfoldM[M, Slice, Option[Key]](None) { key =>
            proj.getBlockAfter(key, constraints).map { b =>
              b.map {
                case BlockProjectionData(_, maxKey, slice) =>
                  (slice, Some(maxKey))
              }
            }
          }
        }

        val stream = projections.foldLeft(StreamT.empty[M, Slice]) {
          (acc, proj) =>
          // FIXME: Can Schema.flatten return Option[Set[ColumnRef]] instead?
          val constraints = proj.structure.map { struct =>
            val set = Schema.flatten(tpe, struct.toList).map {
              case (p, t) => ColumnRef(p, t)
            }.toSet
            Some(set)
          }
          acc ++ StreamT.wrapEffect(constraints map { c => slices(proj, c) })
        }
        Table(stream, ExactSize(totalLength))
      }
    }
  }
}

//object SliceColumnarTableModule {
//  /**
//   * Find the minimal set of projections (and the relevant columns from each projection) that
//   * will be loaded to provide a dataset of the specified type.
//   */
//  protected def minimalCover(tpe: JType, descriptors: Set[ProjectionDescriptor]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
//    @inline @tailrec
//    def cover0(uncovered: Set[ColumnDescriptor], unused: Map[ProjectionDescriptor, Set[ColumnDescriptor]], covers: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
//      if (uncovered.isEmpty) {
//        covers
//      } else {
//        val (b0, covered) = unused map { case (b, dcols) => (b, dcols & uncovered) } maxBy { _._2.size } 
//        cover0(uncovered &~ covered, unused - b0, covers + (b0 -> covered))
//      }
//    }
//
//    cover0(
//      descriptors.flatMap(_.columns.toSet) filter { cd => includes(tpe, cd.selector, cd.valueType) }, 
//      descriptors map { b => (b, b.columns.toSet) } toMap, 
//      Map.empty)
//  }
//
//  /** 
//   * Determine the set of all projections that could potentially provide columns
//   * representing the requested dataset.
//   */
//  protected def loadable[M[+_]: Monad](metadataView: StorageMetadata[M], path: Path, prefix: CPath, jtpe: JType): M[Set[(ProjectionDescriptor, ColumnMetadata)]] = {
//    jtpe match {
//      case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map { _.flatten }
//
//      case JArrayFixedT(elements) =>
//        if (elements.isEmpty) {
//          metadataView.findProjections(path, prefix, CEmptyArray) map { _.toSet }
//        } else {
//          (elements map { case (i, jtpe) => loadable(metadataView, path, prefix \ i, jtpe) } toSet).sequence map { _.flatten }
//        }
//
//      case JArrayUnfixedT =>
//        val emptyM = metadataView.findProjections(path, prefix, CEmptyArray) map { _.toSet }
//        val nonEmptyM = metadataView.findProjections(path, prefix) map { sources =>
//          sources.toSet filter { 
//            _._1.columns exists { 
//              case ColumnDescriptor(`path`, selector, _, _) => 
//                (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[CPathIndex])
//            }
//          }
//        }
//
//        for (empty <- emptyM; nonEmpty <- nonEmptyM) yield empty ++ nonEmpty
//
//      case JObjectFixedT(fields) =>
//        if (fields.isEmpty) {
//          metadataView.findProjections(path, prefix, CEmptyObject) map { _.toSet }
//        } else {
//          (fields map { case (n, jtpe) => loadable(metadataView, path, prefix \ n, jtpe) } toSet).sequence map { _.flatten }
//        }
//
//      case JObjectUnfixedT =>
//        val emptyM = metadataView.findProjections(path, prefix, CEmptyObject) map { _.toSet }
//        val nonEmptyM = metadataView.findProjections(path, prefix) map { sources =>
//          sources.toSet filter { 
//            _._1.columns exists { 
//              case ColumnDescriptor(`path`, selector, _, _) => 
//                (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[CPathField])
//            }
//          }
//        }
//
//        for (empty <- emptyM; nonEmpty <- nonEmptyM) yield empty ++ nonEmpty
//
//      case JArrayHomogeneousT(_) => sys.error("todo")
//
//      case JUnionT(tpe1, tpe2) =>
//        (Set(loadable(metadataView, path, prefix, tpe1), loadable(metadataView, path, prefix, tpe2))).sequence map { _.flatten }
//    }
//  }
//}
// vim: set ts=4 sw=4 et:
