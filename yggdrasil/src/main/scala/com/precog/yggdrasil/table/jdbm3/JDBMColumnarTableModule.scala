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
package jdbm3

import com.precog.common.{MetadataStats,Path,VectorCase}
import com.precog.common.json._
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

trait JDBMColumnarTableModule[M[+_]] extends BlockStoreColumnarTableModule[M] with StorageModule[M] {
  import JDBMColumnarTableModule._

  type Key
  type Projection <: BlockProjectionLike[Key, Slice]
  type TableCompanion <: JDBMColumnarTableCompanion

  trait JDBMColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    type BD = BlockProjectionData[Key,Slice]
  
    private object loadMergeEngine extends MergeEngine[Key, BD]

    def load(table: Table, apiKey: APIKey, tpe: JType): M[Table] = {
      import loadMergeEngine._

      val metadataView = storage.userMetadataView(apiKey)

      def cellsM(projections: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Stream[M[Option[CellState]]] = {
        for (((desc, cols), i) <- projections.toStream.zipWithIndex) yield {
          val succ: Option[Key] => M[Option[BD]] = (key: Option[Key]) => storage.projection(desc) map {
            case (projection, release) => try {
              val result = projection.getBlockAfter(key, cols)
              release.release.unsafePerformIO
              result
            } catch {
              case t: Throwable => blockModuleLogger.error("Error in cell fetch", t); throw t
            }
          }

          succ(None) map { 
            _ map { nextBlock => CellState(i, nextBlock.maxKey, nextBlock.data, (k: Key) => succ(Some(k))) }
          }
        }
      }

      // In order to get a size, we pre-run the metadata fetch
      for {
        paths          <- pathsM(table)
        projectionData <- (paths map { path => loadable(metadataView, path, CPath.Identity, tpe) }).sequence map { _.flatten }
        val (coveringProjections, colMetadata) = projectionData.unzip
        val projectionSizes = colMetadata.toList.flatMap { _.values.flatMap { _.values.collect { case stats: MetadataStats => stats.count } } }.sorted
        val tableSize: TableSize = projectionSizes.headOption.flatMap { minSize => projectionSizes.lastOption.map { maxSize => {
          if (coveringProjections.size == 1) {
            ExactSize(minSize)
          } else {
            EstimateSize(minSize, maxSize)
          }
        }}}.getOrElse(UnknownSize)
      } yield {
        val head = StreamT.Skip(
          StreamT.wrapEffect(
            for {
              cellOptions    <- cellsM(minimalCover(tpe, coveringProjections)).sequence
            } yield {
              mergeProjections(SortAscending, // Projections are always sorted in ascending identity order
                               cellOptions.flatMap(a => a)) { slice => 
                slice.columns.keys map (_.selector) filter (_.nodes.startsWith(CPathField("key") :: Nil))
              }
            }
          )
        )
    
        Table(StreamT(M.point(head)), tableSize)
      }
    }
  }
}

object JDBMColumnarTableModule {
  /**
   * Find the minimal set of projections (and the relevant columns from each projection) that
   * will be loaded to provide a dataset of the specified type.
   */
  protected def minimalCover(tpe: JType, descriptors: Set[ProjectionDescriptor]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
    @inline @tailrec
    def cover0(uncovered: Set[ColumnDescriptor], unused: Map[ProjectionDescriptor, Set[ColumnDescriptor]], covers: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Map[ProjectionDescriptor, Set[ColumnDescriptor]] = {
      if (uncovered.isEmpty) {
        covers
      } else {
        val (b0, covered) = unused map { case (b, dcols) => (b, dcols & uncovered) } maxBy { _._2.size } 
        cover0(uncovered &~ covered, unused - b0, covers + (b0 -> covered))
      }
    }

    cover0(
      descriptors.flatMap(_.columns.toSet) filter { cd => includes(tpe, cd.selector, cd.valueType) }, 
      descriptors map { b => (b, b.columns.toSet) } toMap, 
      Map.empty)
  }

  /** 
   * Determine the set of all projections that could potentially provide columns
   * representing the requested dataset.
   */
  protected def loadable[M[+_]: Monad](metadataView: StorageMetadata[M], path: Path, prefix: CPath, jtpe: JType): M[Set[(ProjectionDescriptor, ColumnMetadata)]] = {
    jtpe match {
      case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map { _.flatten }

      case JArrayFixedT(elements) =>
        if (elements.isEmpty) {
          metadataView.findProjections(path, prefix, CEmptyArray) map { _.toSet }
        } else {
          (elements map { case (i, jtpe) => loadable(metadataView, path, prefix \ i, jtpe) } toSet).sequence map { _.flatten }
        }

      case JArrayUnfixedT =>
        val emptyM = metadataView.findProjections(path, prefix, CEmptyArray) map { _.toSet }
        val nonEmptyM = metadataView.findProjections(path, prefix) map { sources =>
          sources.toSet filter { 
            _._1.columns exists { 
              case ColumnDescriptor(`path`, selector, _, _) => 
                (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[CPathIndex])
            }
          }
        }

        for (empty <- emptyM; nonEmpty <- nonEmptyM) yield empty ++ nonEmpty

      case JObjectFixedT(fields) =>
        if (fields.isEmpty) {
          metadataView.findProjections(path, prefix, CEmptyObject) map { _.toSet }
        } else {
          (fields map { case (n, jtpe) => loadable(metadataView, path, prefix \ n, jtpe) } toSet).sequence map { _.flatten }
        }

      case JObjectUnfixedT =>
        val emptyM = metadataView.findProjections(path, prefix, CEmptyObject) map { _.toSet }
        val nonEmptyM = metadataView.findProjections(path, prefix) map { sources =>
          sources.toSet filter { 
            _._1.columns exists { 
              case ColumnDescriptor(`path`, selector, _, _) => 
                (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[CPathField])
            }
          }
        }

        for (empty <- emptyM; nonEmpty <- nonEmptyM) yield empty ++ nonEmpty

      case JUnionT(tpe1, tpe2) =>
        (Set(loadable(metadataView, path, prefix, tpe1), loadable(metadataView, path, prefix, tpe2))).sequence map { _.flatten }
    }
  }
}
// vim: set ts=4 sw=4 et:
