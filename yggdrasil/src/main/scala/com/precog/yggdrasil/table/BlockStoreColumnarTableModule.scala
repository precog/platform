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
import org.apache.jdbm.DB

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
          if (position == 0) {
            // If we never read from the slice, just return an empty slice and ourself 
            (finished, Cell(index, maxKey, continuing)(succ0))
          } else {
            (finished.sparsen(remap, remap(position - 1) + 1), Cell(index, maxKey, continuing)(succ0)) 
          }
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

      def mergeProjections(cellsM: Set[M[Option[Cell]]], invert: Boolean = false)(keyf: Slice => List[ColumnRef]): M[Table] = {
        for (cells <- cellsM.sequence.map(_.flatten)) yield {
          val cellMatrix = CellMatrix(cells)(keyf)
  
          table(
            StreamT.unfoldM[M, Slice, mutable.PriorityQueue[Cell]](mutable.PriorityQueue(cells.toSeq: _*)(if (invert) cellMatrix.ordering.reverse else cellMatrix.ordering)) { queue =>
  
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
                  val columns: Map[ColumnRef, Column] = {
                    (completeSlices.flatMap(_.columns) ++ prefixes.flatMap(_.columns)).groupBy(_._1).map {
                      case (ref, columns) => {
                        val cp: Pair[ColumnRef, Column] = if (columns.size == 1) {
                          columns.head
                        } else {
                          (ref, ArraySetColumn(ref.ctype, columns.map(_._2).toArray))
                        }
                        cp
                      }
                    }
                  } 
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
        result              <- mergeProjections(cellsM(minimalCover(tpe, coveringProjections))) { slice => 
            //todo: How do we actually determine the correct function for retrieving the key?
            slice.columns.keys.filter( { case ColumnRef(selector, ctype) => selector.nodes.startsWith(JPathField("key") :: Nil) }).toList.sorted
          }
      } yield result
    }

    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): M[Table] = groupByN(Seq(sortKey), Leaf(Source), sortOrder).map(_.head)

    private type SortingKey = Array[Byte]
    private type SortBlockData = BlockProjectionData[SortingKey,Slice]
    private object sortMergeEngine extends MergeEngine[SortingKey, SortBlockData]

    override def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending): M[Seq[Table]] = {
      import sortMergeEngine._

      // Bookkeeping types/case classes
      type IndexStore = SortedMap[Array[Byte],Array[Byte]]
      case class SliceIndex(name: String, storage: IndexStore, sortRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef])

      // Map each group key index and slice column pair to a given JDBM DB index,
      // since each slice could vary in column formats, names, etc
      type IndexMap = Map[(Int, Array[ColumnRef], Array[ColumnRef]), SliceIndex]

      case class DumpState(db: DB, indices: IndexMap, globalId: Long, transforms: List[(Int, SliceTransform1[_])])

      // Open a JDBM3 DB for use in sorting under a temp directory
      val dbFile = new File(newScratchDir(), "groupByNSpace")
      val codec = ColumnCodec.readOnly

      def dumpTables[A](valueTrans: SliceTransform1[A], slices: StreamT[M, Slice], state: DumpState): M[IndexMap] = {
        slices.uncons flatMap {
          _ map {
            case (slice, tail) => {
              val (va0, vslice) = valueTrans(slice)
              val vColumns = vslice.columns.toSeq.sortBy(_._1).toArray
              val vColumnRefs = vColumns.map(_._1)

              val newState = state.transforms.foldLeft(state.copy(transforms = Nil)) { 
                case (DumpState(db, indices, firstGlobalRowId, newTransforms), (i, SliceTransform1(a, f))) =>
                  val (a0, slice0) = f(a, slice)
                  val nextTransform = SliceTransform1(a0, f)
                  
                  val keyColumns = slice0.columns.toSeq.sortBy(_._1).toArray
                  val keyColumnRefs = keyColumns.map(_._1)

                  val indexMapKey = (i, keyColumnRefs, vColumnRefs)

                  val (index, newIndices) = indices.get(indexMapKey) map { (_, indices) } getOrElse {
                    val indexName = i + "-" + indexMapKey.toString
                    val newIndex = SliceIndex(indexName,
                                              db.createTreeMap(indexName,
                                                               SortingKeyComparator(sortOrder.isAscending),
                                                               null /* use default serialization for Array[Byte] */,
                                                               null /* Use default serialization for Array[Byte] */),
                                              keyColumnRefs,
                                              vColumnRefs)

                    (newIndex, indices + (indexMapKey -> newIndex))
                  }

                  // Iterate over the slice, storing each row
                  @inline
                  @tailrec
                  def storeRow(storage: IndexStore, row: Int, globalId: Long): Long = if (row < slice.size) {
                    storage.put(codec.encodeSortColumns(keyColumns, row, globalId), codec.encodeRawColumns(vColumns, row))

                    if (globalId % jdbmCommitInterval == 0 && globalId > 0) {
                      db.commit()
                    }

                    storeRow(storage, row + 1, globalId + 1)
                  } else {
                    globalId
                  }

                  DumpState(db, newIndices, storeRow(index.storage, 0, firstGlobalRowId), (i, nextTransform) :: newTransforms)
              }

              dumpTables(valueTrans, tail, newState)
            }
          } getOrElse {
            // No more slices, close out the JDBM database
            state.db.close()
            M.point(state.indices)
          }
        }
      }

      // A little ugly, but getting this implicitly seemed a little crazy
      val sortingKeyOrder: Order[SortingKey] = scalaz.Order.fromScalaOrdering(
        scala.math.Ordering.comparatorToOrdering(
          SortingKeyComparator(sortOrder.isAscending)
        )
      )

      dumpTables(
        composeSliceTransform(valueSpec),
        slices,
        DumpState(DBMaker.openFile(dbFile.getCanonicalPath).make(), Map.empty, 0l, (groupKeys map composeSliceTransform).zipWithIndex.map(_.swap).toList)
      ) flatMap {
        indices => {
          (indices.groupBy(_._1._1).map {
            case (_, backingIndices) => {
              // Map the distinct indices into SortProjections/Cells, then merge them
              val cells: Set[M[Option[Cell]]] = backingIndices.zipWithIndex.map {
                case ((_, SliceIndex(name, _, keyColumns, valColumns)), index) => {
                  val sortProjection = new JDBMRawSortProjection(dbFile, name, keyColumns, valColumns) {
                    def keyOrder: Order[SortingKey] = sortingKeyOrder
                  }

                  val succ: Option[SortingKey] => M[Option[SortBlockData]] = (key: Option[SortingKey]) => M.point(sortProjection.getBlockAfter(key))

                  succ(None) map { 
                    _ map { nextBlock => Cell(index, nextBlock.maxKey, nextBlock.data) { k => succ(Some(k)) } }
                  }
                }
              }.toSet

              mergeProjections(cells, sortOrder.isAscending) { slice => {
                // only need to compare on the group keys (0th element of resulting table) between projections
                slice.columns.keys.filter({ case ColumnRef(selector, _) => selector.nodes.startsWith(JPathIndex(0) :: Nil) }).toList.sorted
              }}
            }
          }).toStream.sequence
        }
      }
    }
  }

  def table(slices: StreamT[M, Slice]) = new Table(slices)
}

// vim: set ts=4 sw=4 et:
