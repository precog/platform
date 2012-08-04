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

import com.precog.common.{Path,VectorCase}
import com.precog.bytecode._
import com.precog.yggdrasil.jdbm3._
import com.precog.util._
import Schema._
import metadata._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import java.io.File
import java.util.SortedMap

import org.apache.jdbm.DBMaker

import scalaz._
import scalaz.Ordering._
import scalaz.std.set._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._
import scalaz.syntax.traverse._
import scalaz.syntax.std.stream._
import scala.annotation.tailrec
import scala.collection.mutable

trait BlockStoreColumnarTableModule[M[+_]] extends ColumnarTableModule[M] with StorageModule[M] { self =>
  override type UserId = String
  type Key
  type Projection <: BlockProjectionLike[Key, Slice]
  type BD = BlockProjectionData[Key,Slice]

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import trans._

    /** 
     * Determine the set of all projections that could potentially provide columns
     * representing the requested dataset.
     */
    def loadable(metadataView: StorageMetadata[M], path: Path, prefix: JPath, jtpe: JType): M[Set[ProjectionDescriptor]] = {
      jtpe match {
        case p: JPrimitiveType => ctypes(p).map(metadataView.findProjections(path, prefix, _)).sequence map {
          sources => 
            sources flatMap { source => source.keySet }
        }

        case JArrayFixedT(elements) =>
          if (elements.isEmpty) {
            metadataView.findProjections(path, prefix, CEmptyArray) map { _.keySet }
          } else {
            (elements map { case (i, jtpe) => loadable(metadataView, path, prefix \ i, jtpe) } toSet).sequence map { _.flatten }
          }

        case JArrayUnfixedT =>
          metadataView.findProjections(path, prefix) map { sources =>
            sources.keySet filter { 
              _.columns exists { 
                case ColumnDescriptor(`path`, selector, _, _) => 
                  (selector dropPrefix prefix).flatMap(_.head).exists(_.isInstanceOf[JPathIndex])
              }
            }
          }

        case JObjectFixedT(fields) =>
          if (fields.isEmpty) {
            metadataView.findProjections(path, prefix, CEmptyObject) map { _.keySet }
          } else {
            (fields map { case (n, jtpe) => loadable(metadataView, path, prefix \ n, jtpe) } toSet).sequence map { _.flatten }
          }

        case JObjectUnfixedT =>
          metadataView.findProjections(path, prefix) map { sources =>
            sources.keySet filter { 
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
          val (b0, covered) = unused map { case (b, dcols) => (b, dcols & uncovered) } maxBy { _._2.size } 
          cover0(uncovered &~ covered, unused - b0, covers + (b0 -> covered))
        }
      }

      cover0(
        descriptors.flatMap(_.columns.toSet) filter { cd => includes(tpe, cd.selector, cd.valueType) }, 
        descriptors map { b => (b, b.columns.toSet) } toMap, 
        Map.empty)
    }

    class MergeEngine[KeyType,BlockData <: BlockProjectionData[KeyType,Slice]] {
      /**
       * A wrapper for a slice, and the function required to get the subsequent
       * block of data.
       */
      case class Cell(index: Int, maxKey: KeyType, slice0: Slice)(succ0: KeyType => M[Option[BlockData]]) {
        val uuid = java.util.UUID.randomUUID()
        private val remap = new Array[Int](slice0.size)
        var position: Int = 0

        def succ: M[Option[Cell]] = {
          for (blockOpt <- succ0(maxKey)) yield {
            blockOpt map { block => 
              Cell(index, block.maxKey, block.data)(succ0) 
            }
          }
        }

        def advance(i: Int): Boolean = {
          if (position < slice0.size) {
            remap(position) = i
            position += 1
          }
          
          position < slice0.size
        }
  
        def slice = {
          slice0.sparsen(remap, remap(position - 1) + 1)
        }
  
        def split: (Slice, Cell) = {
          val (finished, continuing) = slice0.split(position)
          (finished.sparsen(remap, remap(position - 1) + 1), Cell(index, maxKey, continuing)(succ0)) 
        }
      }
  
      sealed trait CellMatrix { self => 
        def cells: Iterable[Cell]
        def compare(cl: Cell, cr: Cell): Ordering
        def refresh(index: Int, succ: M[Option[Cell]]): M[CellMatrix]
  
        implicit lazy val ordering = new scala.math.Ordering[Cell] {
          def compare(c1: Cell, c2: Cell) = self.compare(c1, c2).toInt
        }
      }
  
      object CellMatrix {
        def apply(initialCells: Set[Cell])(keyf: Slice => List[ColumnRef]): CellMatrix = {
          val size = initialCells.size
  
          type ComparatorMatrix = Array[Array[(Int, Int) => Ordering]]
          def fillMatrix(initialCells: Set[Cell]): ComparatorMatrix = {
            val comparatorMatrix = Array.ofDim[(Int, Int) => Ordering](initialCells.size, initialCells.size)
  
            for (Cell(i, _, s) <- initialCells; Cell(i0, _, s0) <- initialCells if i != i0) { 
              comparatorMatrix(i)(i0) = Slice.rowComparator(s, s0)(keyf) 
            }
  
            comparatorMatrix
          }
  
          new CellMatrix { self =>
            private[this] val allCells: mutable.Map[Int, Cell] = initialCells.map(c => (c.index, c))(collection.breakOut)
            private[this] val comparatorMatrix = fillMatrix(initialCells)
  
            def cells = allCells.values
  
            def compare(cl: Cell, cr: Cell): Ordering = {
              comparatorMatrix(cl.index)(cr.index)(cl.position, cr.position)
            }
  
            def refresh(index: Int, succ: M[Option[Cell]]): M[CellMatrix] = {
              succ map {
                case Some(c @ Cell(i, _, s)) => 
                  allCells += (i -> c)
  
                  for ((_, Cell(i0, _, s0)) <- allCells if i0 != i) {
                    comparatorMatrix(i)(i0) = Slice.rowComparator(s, s0)(keyf) 
                    comparatorMatrix(i0)(i) = Slice.rowComparator(s0, s)(keyf)
                  }
  
                  self
                  
                case None => 
                  allCells -= index
  
                  // this is basically so that we'll fail fast if something screwy happens
                  for ((_, Cell(i0, _, s0)) <- allCells if i0 != index) {
                    comparatorMatrix(index)(i0) = null
                    comparatorMatrix(i0)(index) = null
                  }
  
                  self
              }
            }
          }
        }
      }

      def mergeProjections(cellsM: Set[M[Option[Cell]]]): M[Table] = {
        for (cells <- cellsM.sequence.map(_.flatten)) yield {
          val cellMatrix = CellMatrix(cells) { slice => 
            //todo: How do we actually determine the correct function for retrieving the key?
            slice.columns.keys.filter( { case ColumnRef(selector, ctype) => selector.nodes.startsWith(JPathField("key") :: Nil) }).toList.sorted
          }
  
          table(
            StreamT.unfoldM[M, Slice, mutable.PriorityQueue[Cell]](mutable.PriorityQueue(cells.toSeq: _*)(cellMatrix.ordering)) { queue =>
  
              // dequeues all equal elements from the head of the queue
              @inline @tailrec def dequeueEqual(cells: List[Cell]): List[Cell] = {
                if (queue.isEmpty) cells
                else if (cells.isEmpty || cellMatrix.compare(queue.head, cells.head) == EQ) dequeueEqual(queue.dequeue() :: cells)
                else cells
              }
  
              // consume as many records as possible
              @inline @tailrec def consumeToBoundary(idx: Int): (Int, List[Cell]) = {
                val cellBlock = dequeueEqual(Nil)
  
                if (cellBlock.isEmpty) {
                  // At the end of data, since this will only occur if nothing remains in the priority queue
                  (idx, Nil)
                } else {
                  val (continuing, expired) = cellBlock partition { _.advance(idx) }
                  queue.enqueue(continuing: _*)
  
                  if (expired.isEmpty) consumeToBoundary(idx + 1) else (idx + 1, expired)
                }
              }
  
              val (finishedSize, expired) = consumeToBoundary(0)
              if (expired.isEmpty) {
                M.point(None)
              } else {
                val completeSlices = expired.map(_.slice)              
                
                val (prefixes, suffixes) = queue.dequeueAll.map(_.split).unzip
  
                val emission = new Slice {
                  val size = finishedSize
                  val columns: Map[ColumnRef, Column] = completeSlices.flatMap(_.columns).toMap ++ prefixes.flatMap(_.columns)
                }
  
                val updatedMatrix = expired.foldLeft(M.point(cellMatrix)) {
                  case (matrixM, cell) => matrixM.flatMap(_.refresh(cell.index, cell.succ))
                }
  
                val updatedMatrix0 = suffixes.foldLeft(updatedMatrix) {
                  case (matrixM, cell) => matrixM.flatMap(_.refresh(cell.index, M.point(Some(cell))))
                }
  
                updatedMatrix0 map { matrix => 
                  val queue0 = mutable.PriorityQueue(matrix.cells.toSeq: _*)(matrix.ordering)
                  Some((emission, queue0))
                }
              }
            }
          )
        }
      }
    }

    private object loadMergeEngine extends MergeEngine[Key,BD]

    def load(uid: UserId, tpe: JType): M[Table] = {
      import loadMergeEngine._
      val metadataView = storage.userMetadataView(uid)

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

      def cellsM(projections: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Set[M[Option[Cell]]] = for (((desc, cols), i) <- projections.toSeq.zipWithIndex.toSet) yield {
        val succ: Option[Key] => M[Option[BD]] = (key: Option[Key]) => storage.projection(desc) map {
          case (projection, release) => 
            val result = projection.getBlockAfter(key, cols)  
            release.release.unsafePerformIO
            result
        }

        succ(None) map { 
          _ map { nextBlock => Cell(i, nextBlock.maxKey, nextBlock.data) { k => succ(Some(k)) } }
        }
      }

      for {
        paths               <- pathsM
        coveringProjections <- (paths map { path => loadable(metadataView, path, JPath.Identity, tpe) }).sequence map { _.flatten }
        result              <- mergeProjections(cellsM(minimalCover(tpe, coveringProjections)))
      } yield result
    }

    private type SortBlockData = BlockProjectionData[SortingKey,Slice]
    private object sortMergeEngine extends MergeEngine[SortingKey, SortBlockData]

    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): M[Table] = {
      import sortMergeEngine._
      import TableModule.paths._

      type IndexStore = SortedMap[SortingKey,Array[CValue]]
      case class SliceIndex(name: String, storage: IndexStore)
      type IndexMap = Map[Seq[ColumnRef], SliceIndex] 

      val tableWithSortKey = transform(ObjectConcat(Leaf(Source), WrapObject(sortKey, SortKey.name)))

      // Open a JDBM3 DB for use in sorting under a temp directory
      val dbFile = new File(newScratchDir(), "sortspace")
      val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
      
      // Insert slice data based on columndescriptors
      val inputOp: M[(IndexMap,Int,Seq[ColumnRef])] = tableWithSortKey.slices.foldLeft((Map.empty[Seq[ColumnRef], SliceIndex], 
                                                                                    Seq.empty[ColumnRef] /* Sort column refs */,
                                                                                    0 /* Identities count */,
                                                                                    0l /* synthetic global row ID */)) { case ((indices, oldSortColumns, _, nextId), slice) => {
        def columnsByPrefix(prefix: JPath): List[(ColumnRef,Column)] = slice.columns.collect {
          // The conditional guarantees that dropPrefix will return Some
          case (ColumnRef(selector, tpe), col) if selector.hasPrefix(prefix) => (ColumnRef(selector.dropPrefix(prefix).get, tpe), col)
        }.toList

        val dataColumns = columnsByPrefix(JPath(Value))
        val idColumns   = columnsByPrefix(JPath(Key))
        val sortColumns = columnsByPrefix(JPath(SortKey))

        var formatIndices = indices
        
        val dataRefs = dataColumns.map(_._1).toSeq
        val index: SliceIndex = formatIndices.get(dataRefs).getOrElse {
          val newIndex = SliceIndex(dataRefs.toString, DB.createTreeMap(dataRefs.toString, 
                                                                        SortingKeyComparator(sortOrder.ascending), 
                                                                        SortingKeySerializer(sortColumns.map(_._1.ctype).toArray, idColumns.size),
                                                                        CValueSerializer(dataColumns.map(_._1.ctype))))

          formatIndices += (dataRefs -> newIndex)
          newIndex
        }
        
        // Iterate over the slice
        @inline
        @tailrec
        def storeRow(storage: IndexStore, row: Int, globalId: Long): Long = if (row < slice.size) {
          val sortValues = sortColumns.map(_._2.cValue(row)).toArray
          val identities = VectorCase(idColumns.map(_._2.asInstanceOf[LongColumn].apply(row)) : _*)
          val rowValues  = dataColumns.map(_._2.cValue(row)).toArray

          storage.put(SortingKey(sortValues, identities, globalId), rowValues)

          if (globalId % jdbmCommitInterval == 0 && globalId > 0) {
            DB.commit()
          }

          storeRow(storage, row + 1, globalId + 1)
        } else {
          globalId
        }

        val newSortColumns = sortColumns.map(_._1).toSeq
        assert(newSortColumns == oldSortColumns) // Sort columns should not change over the span of all slices in the table
        (formatIndices, newSortColumns, idColumns.size, storeRow(index.storage, 0, nextId))
      }}.map {
        case (indices, sortColumns, idCount, lastId) => DB.close(); logger.debug("Sorted %d rows to JDBM".format(lastId - 1)); (indices, idCount, sortColumns)
      }

      val sortingKeyOrder: Order[SortingKey] = scalaz.Order.fromScalaOrdering(scala.math.Ordering.comparatorToOrdering(SortingKeyComparator(sortOrder.ascending)))

      // Merge the resulting slice indices back together
      inputOp.flatMap { case (indices,idCount,sortCols) => {
        // Map the distinct indices into SortProjections/Cells, then merge them
        val cells: Set[M[Option[Cell]]] = indices.zipWithIndex.map {
          case ((format,SliceIndex(name, _)),index) => {
            val sortProjection = new JDBMRawSortProjection(dbFile, name, idCount, sortCols, format) {
              def keyOrder: Order[SortingKey] = sortingKeyOrder
            }

            val succ: Option[SortingKey] => M[Option[SortBlockData]] = (key: Option[SortingKey]) => M.point(sortProjection.getBlockAfter(key))

            succ(None) map { 
              _ map { nextBlock => Cell(index, nextBlock.maxKey, nextBlock.data) { k => succ(Some(k)) } }
            }
          }
        }.toSet

        mergeProjections(cells)
      }}
    }
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
