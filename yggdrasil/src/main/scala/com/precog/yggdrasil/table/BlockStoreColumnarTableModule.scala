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
import com.precog.yggdrasil.util._
import com.precog.util._
import Schema._
import metadata._

import blueeyes.json.{JPath,JPathField,JPathIndex}

import java.io.File
import java.util.SortedMap
import java.util.Comparator

import org.apache.jdbm.DBMaker
import org.apache.jdbm.DB

import com.weiglewilczek.slf4s.Logger

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
trait BlockStoreColumnarTableModule[M[+_]] extends
  ColumnarTableModule[M] with
  StorageModule[M] with
  IdSourceScannerModule[M] { self =>

  import trans._
  import TransSpec.deepMap
  import SliceTransform._
  import BlockStoreColumnarTableModule._
    
  override type UserId = String
  type Key
  type Projection <: BlockProjectionLike[Key, Slice]
  type TableCompanion <: BlockStoreColumnarTableCompanion

  type BD = BlockProjectionData[Key,Slice]
  
  def newMemoContext = new MemoContext
  
  class MemoContext extends MemoizationContext {
    import trans._
    
    private val memoCache = mutable.HashMap.empty[MemoId, M[Table]]
    
    private val memoKey   = "MemoKey"
    private val memoValue = "MemoValue"
    
    def memoize(table: Table, memoId: MemoId): M[Table] = {
      val preMemoTable =
        table.transform(
          ObjectConcat(
            WrapObject(
              Scan(
                ConstLiteral(CLong(0), Leaf(Source)),
                freshIdScanner),
              memoKey),
            WrapObject(
              Leaf(Source),
              memoValue
            )
          )
        )
      
      val memoTable = sort(preMemoTable, DerefObjectStatic(Leaf(Source), JPathField(memoKey)), SortAscending, memoId)
      M.map(memoTable) { _.transform(DerefObjectStatic(Leaf(Source), JPathField(memoValue))) }
    }
    
    def sort(table: Table, sortKey: TransSpec1, sortOrder: DesiredSortOrder, memoId: MemoId, unique: Boolean = true): M[Table] = {
      // yup, we still block the whole world. Yay.
      memoCache.synchronized {
        memoCache.get(memoId) match {
          case Some(memoTable) => memoTable
          case None =>
            val memoTable = table.sort(sortKey, sortOrder, unique)
            memoCache += (memoId -> memoTable)
            memoTable
        }
      }
    }
    
    def expire(memoId: MemoId): Unit =
      memoCache.synchronized {
        memoCache -= memoId
      }
    
    def purge() : Unit =
      memoCache.synchronized {
        memoCache.clear()
      }
  }

  private class MergeEngine[KeyType, BlockData <: BlockProjectionData[KeyType, Slice]] {
    case class CellState(index: Int, maxKey: KeyType, slice0: Slice, succf: KeyType => M[Option[BlockData]], remap: Array[Int], position: Int) {
      def toCell = {
        new Cell(index, maxKey, slice0)(succf, remap.clone, position)
      }
    }
  

    object CellState {
      def apply(index: Int, maxKey: KeyType, slice0: Slice, succf: KeyType => M[Option[BlockData]]) = {
        val remap = new Array[Int](slice0.size)
        new CellState(index, maxKey, slice0, succf, remap, 0)
      }
    }


    /**
     * A wrapper for a slice, and the function required to get the subsequent
     * block of data.
     */
    case class Cell private[MergeEngine] (index: Int, maxKey: KeyType, slice0: Slice)(succf: KeyType => M[Option[BlockData]], remap: Array[Int], var position: Int) {
                     
      def advance(i: Int): Boolean = {
        if (position < slice0.size) {
          remap(position) = i
          position += 1
        }
        
        position < slice0.size
      }

      def slice = {
        slice0.sparsen(remap, if (position > 0) remap(position - 1) + 1 else 0)
      }

      def succ: M[Option[CellState]] = {
        for (blockOpt <- succf(maxKey)) yield {
          blockOpt map { block => CellState(index, block.maxKey, block.data, succf) }
        }
      }

      def split: (Slice, CellState) = {
        val (finished, continuing) = slice0.split(position)
        val nextState = CellState(index, maxKey, continuing, succf)
        (if (position == 0) finished else finished.sparsen(remap, remap(position - 1) + 1), nextState)
      }

      // Freeze the state of this cell. Used to ensure restartability from any point in a stream of slices derived
      // from mergeProjections.
      def state: CellState = {
        val remap0 = new Array[Int](slice0.size)
        System.arraycopy(remap, 0, remap0, 0, slice0.size) 
        new CellState(index, maxKey, slice0, succf, remap0, position)
      }
    }

    sealed trait CellMatrix { self => 
      def cells: Iterable[Cell]
      def compare(cl: Cell, cr: Cell): Ordering

      implicit lazy val ordering = new scala.math.Ordering[Cell] {
        def compare(c1: Cell, c2: Cell) = self.compare(c1, c2).toInt
      }
    }

    object CellMatrix {
      def apply(initialCells: Vector[Cell])(keyf: Slice => List[ColumnRef]): CellMatrix = {
        val size = if (initialCells.isEmpty) 0 else initialCells.map(_.index).max + 1
        
        type ComparatorMatrix = Array[Array[RowComparator]]
        def fillMatrix(initialCells: Vector[Cell]): ComparatorMatrix = {
          val comparatorMatrix = Array.ofDim[RowComparator](size, size)

          for (Cell(i, _, s) <- initialCells; Cell(i0, _, s0) <- initialCells if i != i0) { 
            comparatorMatrix(i)(i0) = Slice.rowComparatorFor(s, s0)(keyf) 
          }

          comparatorMatrix
        }

        new CellMatrix { self =>
          private[this] val allCells: mutable.Map[Int, Cell] = initialCells.map(c => (c.index, c))(collection.breakOut)
          private[this] val comparatorMatrix = fillMatrix(initialCells)

          def cells = allCells.values

          def compare(cl: Cell, cr: Cell): Ordering = {
            comparatorMatrix(cl.index)(cr.index).compare(cl.position, cr.position)
          }
        }
      }
    }

    def mergeProjections(cellStates: Stream[CellState], invert: Boolean)(keyf: Slice => List[ColumnRef]): StreamT[M, Slice] = {
      StreamT.unfoldM[M, Slice, Stream[CellState]](cellStates) { cellStates => 
        val cells: Vector[Cell] = cellStates.map(_.toCell)(collection.breakOut)

        // TODO: We should not recompute all of the row comparators every time, since all but one will
        // still be valid and usable. However, getting to this requires more significant rework than can be
        // undertaken right now.
        val cellMatrix = CellMatrix(cells)(keyf)
        val queue = mutable.PriorityQueue(cells.toSeq: _*)(if (invert) cellMatrix.ordering.reverse else cellMatrix.ordering)
        
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

          val successorStatesM = expired.map(_.succ).sequence.map(_.toStream.collect({case Some(cs) => cs}))

          successorStatesM map { successorStates => 
            Some((emission, successorStates ++ suffixes))
          }
        }
      }
    }
  }

  trait BlockStoreColumnarTableCompanion extends ColumnarTableCompanion {
    import SliceTransform._

    type SortingKey = Array[Byte]
    type SortBlockData = BlockProjectionData[SortingKey,Slice]

    type IndexStore = SortedMap[SortingKey, Array[Byte]]
    case class SliceIndex(name: String, storage: IndexStore, keyComparator: Comparator[SortingKey], keyRefs: Array[ColumnRef], valRefs: Array[ColumnRef])

    case class IndexKey(streamId: String, keyRefs: List[ColumnRef], valRefs: List[ColumnRef]) {
      val name = streamId + ";krefs=" + keyRefs.mkString("[", ",", "]") + ";vrefs=" + valRefs.mkString("[", ",", "]")
    }

    type IndexMap = Map[IndexKey, SliceIndex]

    case class JDBMState(indices: IndexMap, insertCount: Long)
    object JDBMState {
      val empty = JDBMState(Map(), 0l)
    }

    case class WriteState(jdbmState: JDBMState, valueTrans: SliceTransform1[_], keyTransforms: Seq[SliceTransform1[_]])

    private[BlockStoreColumnarTableModule] object loadMergeEngine extends MergeEngine[Key, BD]
    private[BlockStoreColumnarTableModule] object sortMergeEngine extends MergeEngine[SortingKey, SortBlockData]

    def apply(slices: StreamT[M, Slice]) = new Table(slices)

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = {
      sealed trait AlignState
      case class RunLeft(rightRow: Int, rightKey: Slice) extends AlignState
      case class RunRight(leftRow: Int, leftKey: Slice) extends AlignState
      case class FindEqualAdvancingRight(leftRow: Int, leftKey: Slice) extends AlignState
      case class FindEqualAdvancingLeft(rightRow: Int, rightKey: Slice) extends AlignState

      sealed trait Span
      case object LeftSpan extends Span
      case object RightSpan extends Span
      case object NoSpan extends Span


      sealed trait NextStep
      case class MoreLeft(span: Span, leq: mutable.BitSet, ridx: Int, req: mutable.BitSet) extends NextStep
      case class MoreRight(span: Span, lidx: Int, leq: mutable.BitSet, req: mutable.BitSet) extends NextStep


      def writeStreams[A, B](dbFile: File, db: DB, 
                             left: StreamT[M, Slice], leftKeyTrans: SliceTransform1[A],
                             right: StreamT[M, Slice], rightKeyTrans: SliceTransform1[B],
                             leftWriteState: JDBMState, rightWriteState: JDBMState): M[(Table, Table)] = {

        // We will *always* have a lhead and rhead, because if at any point we run out of data,
        // we'll still be hanging on to the last slice on the other side to use as the authority
        // for equality comparisons
        def step(state: AlignState, lhead: Slice, ltail: StreamT[M, Slice], stepleq: mutable.BitSet,
                                    rhead: Slice, rtail: StreamT[M, Slice], stepreq: mutable.BitSet,
                                    lstate: A, rstate: B, 
                                    leftWriteState: JDBMState, rightWriteState: JDBMState): M[(JDBMState, JDBMState)] = {

          @tailrec def buildFilters(comparator: RowComparator, 
                                    lidx: Int, lsize: Int, lacc: mutable.BitSet, 
                                    ridx: Int, rsize: Int, racc: mutable.BitSet,
                                    span: Span): NextStep = {

            //println(span)

            // todo: This is optimized for sparse alignments; if you get into an alignment
            // where every pair is distinct and equal, you'll do 2*n comparisons.
            // This should instead be optimized for dense alignments, using an algorithm that
            // advances both sides after an equal, then backtracks on inequality
            if (span eq LeftSpan) {
              // We don't need to compare the index on the right, since it will be left unchanged
              // throughout the time that we're advancing left, and even if it's beyond the end of
              // input we can use the next-to-last element for comparison
              
              if (lidx < lsize) {
                comparator.compare(lidx, ridx - 1) match {
                  case EQ => 
                    //println("Found equal on left.")
                    buildFilters(comparator, lidx + 1, lsize, lacc + lidx, ridx, rsize, racc, LeftSpan)
                  case LT => 
                    sys.error("Inputs to align are not correctly sorted.")
                  case GT =>
                    buildFilters(comparator, lidx, lsize, lacc, ridx, rsize, racc, NoSpan)
                }
              } else {
                // left is exhausted in the midst of a span
                //println("Left exhausted in the middle of a span.")
                MoreLeft(LeftSpan, lacc, ridx, racc)
              }
            } else {
              if (lidx < lsize && ridx < rsize) {
                comparator.compare(lidx, ridx) match {
                  case EQ => 
                    //println("Found equal on right.")
                    buildFilters(comparator, lidx, lsize, lacc, ridx + 1, rsize, racc + ridx, RightSpan)
                  case LT => 
                    if (span eq RightSpan) {
                      // drop into left spanning of equal
                      buildFilters(comparator, lidx, lsize, lacc, ridx, rsize, racc, LeftSpan)
                    } else {
                      // advance the left in the not-left-spanning state
                      buildFilters(comparator, lidx + 1, lsize, lacc, ridx, rsize, racc, NoSpan)
                    }
                  case GT =>
                    if (span eq RightSpan) sys.error("Inputs to align are not correctly sorted")
                    else buildFilters(comparator, lidx, lsize, lacc, ridx + 1, rsize, racc, NoSpan)
                }
              } else if (lidx < lsize) {
                // right is exhausted; span will be RightSpan or NoSpan
                //println("Right exhausted, left is not; asking for more right with " + lacc.mkString("[", ",", "]") + ";" + racc.mkString("[", ",", "]") )
                MoreRight(span, lidx, lacc, racc)
              } else {
                //println("Both sides exhausted, so emitting with " + lacc.mkString("[", ",", "]") + ";" + racc.mkString("[", ",", "]") )
                MoreLeft(NoSpan, lacc, ridx, racc)
              }
            }
          }

          def continue(nextStep: NextStep, comparator: RowComparator, lstate: A, lkey: Slice, rstate: B, rkey: Slice, leftWriteState: JDBMState, rightWriteState: JDBMState): M[(JDBMState, JDBMState)] = nextStep match {
            case MoreLeft(span, leq, ridx, req) =>
              //println("Requested more left; emitting left based on bitset " + leq.mkString("[", ",", "]"))
              val lemission = leq.nonEmpty.option(lhead.filterColumns(cf.util.filter(0, lhead.size, leq)))

              @inline def next(lbs: JDBMState, rbs: JDBMState): M[(JDBMState, JDBMState)] = ltail.uncons flatMap {
                case Some((lhead0, ltail0)) =>
                  //println("Continuing on left; not emitting right.")
                  val nextState = (span: @unchecked) match {
                    case NoSpan => FindEqualAdvancingLeft(ridx, rkey)
                    case LeftSpan => RunLeft(ridx, rkey)
                  }

                  step(nextState, lhead0, ltail0, new mutable.BitSet(), rhead, rtail, req, lstate, rstate, lbs, rbs)
                case None =>
                  //println("No more data on left; emitting right based on bitset " + req.mkString("[", ",", "]"))
                  // done on left, and we're not in an equal span on the right (since LeftSpan can only
                  // be emitted if we're not in a right span) so we're entirely done.
                  val remission = req.nonEmpty.option(rhead.filterColumns(cf.util.filter(0, rhead.size, req))) 
                  (remission map { e => writeAlignedSlices(db, rkey, e, rbs, "alignRight", SortAscending) } getOrElse rbs.point[M]) map { (lbs, _) }
              }

              lemission map { e => 
                writeAlignedSlices(db, lkey, e, leftWriteState, "alignLeft", SortAscending) flatMap { next(_: JDBMState, rightWriteState) }
              } getOrElse {
                next(leftWriteState, rightWriteState)
              }

            case MoreRight(span, lidx, leq, req) =>
              //println("Requested more right; emitting right based on bitset " + req.mkString("[", ",", "]"))
              // if span == RightSpan and no more data exists on the right, we need to 
              // continue in buildFilters spanning on the left.
              val remission = req.nonEmpty.option(rhead.filterColumns(cf.util.filter(0, rhead.size, req)))

              @inline def next(lbs: JDBMState, rbs: JDBMState): M[(JDBMState, JDBMState)] = rtail.uncons flatMap {
                case Some((rhead0, rtail0)) => 
                  //println("Continuing on right.")
                  val nextState = (span: @unchecked) match {
                    case NoSpan => FindEqualAdvancingRight(lidx, lkey)
                    case RightSpan => RunRight(lidx, lkey)
                  }

                  step(nextState, lhead, ltail, leq, rhead0, rtail0, new mutable.BitSet(), lstate, rstate, lbs, rbs)

                case None =>
                  // no need here to check for LeftSpan by the contract of buildFilters
                  (span: @unchecked) match {
                    case NoSpan => 
                      //println("No more data on right and not in a span; emitting left based on bitset " + leq.mkString("[", ",", "]"))
                      // entirely done; just emit both 
                      val lemission = leq.nonEmpty.option(lhead.filterColumns(cf.util.filter(0, lhead.size, leq)))
                      (lemission map { e => writeAlignedSlices(db, lkey, e, lbs, "alignLeft", SortAscending) } getOrElse lbs.point[M]) map { (_, rbs) }

                    case RightSpan => 
                      // need to switch to left spanning in buildFilters
                      //println("No more data on right, but in a span so continuing on left.")
                      val nextState = buildFilters(comparator, lidx, lhead.size, leq, rhead.size, rhead.size, new mutable.BitSet(), LeftSpan)
                      continue(nextState, comparator, lstate, lkey, rstate, rkey, lbs, rbs)
                  }
              }

              remission map { e => 
                writeAlignedSlices(db, rkey, e, rightWriteState, "alignRight", SortAscending) flatMap { next(leftWriteState, _: JDBMState) }
              } getOrElse {
                next(leftWriteState, rightWriteState)
              }
          }


          // this is an optimization that uses a preemptory comparison and a binary
          // search to skip over big chunks of (or entire) slices if possible.
          def findEqual(comparator: RowComparator, leftRow: Int, leq: mutable.BitSet, rightRow: Int, req: mutable.BitSet): NextStep = {
            comparator.compare(leftRow, rightRow) match {
              case EQ => 
                buildFilters(comparator, leftRow, lhead.size, leq, rightRow, rhead.size, req, NoSpan)

              case LT => 
                val leftIdx = comparator.nextLeftIndex(lhead.size - 1, lhead.size, 0, lhead.size - leftRow - 1)
                if (leftIdx == lhead.size) {
                  MoreLeft(NoSpan, leq, rightRow, req)
                } else {
                  buildFilters(comparator, leftIdx, lhead.size, leq, rightRow, rhead.size, req, NoSpan)
                }
            
              case GT => 
                val rightIdx = comparator.swap.nextLeftIndex(rhead.size - 1, rhead.size, 0, rhead.size - rightRow - 1)
                if (rightIdx == rhead.size) {
                  MoreRight(NoSpan, leftRow, leq, req)
                } else {
                  // do a binary search to find the indices where the comparison becomse LT or EQ
                  buildFilters(comparator, leftRow, lhead.size, leq, rightIdx, rhead.size, req, NoSpan)
                }
            }
          }

          state match {
            case FindEqualAdvancingRight(leftRow, lkey) => 
              // whenever we drop into buildFilters in this case, we know that we will be neither
              // in a left span nor a right span because we didn't have an equal case at the
              // last iteration.

              val (nextB, rkey) = rightKeyTrans.f(rstate, rhead)
              val comparator = Slice.rowComparatorFor(lkey, rkey) { s => s.columns.keys.toList.sorted }
              
              // do some preliminary comparisons to figure out if we even need to look at the current slice
              val nextState = findEqual(comparator, leftRow, stepleq, 0, stepreq)
              continue(nextState, comparator, lstate, lkey, nextB, rkey, leftWriteState, rightWriteState)    
            
            case FindEqualAdvancingLeft(rightRow, rkey) => 
              // whenever we drop into buildFilters in this case, we know that we will be neither
              // in a left span nor a right span because we didn't have an equal case at the
              // last iteration.

              val (nextA, lkey) = leftKeyTrans.f(lstate, lhead)
              val comparator = Slice.rowComparatorFor(lkey, rkey) { s => s.columns.keys.toList.sorted }
              
              // do some preliminary comparisons to figure out if we even need to look at the current slice
              val nextState = findEqual(comparator, 0, stepleq, rightRow, stepreq)
              continue(nextState, comparator, nextA, lkey, rstate, rkey, leftWriteState, rightWriteState)    
            
            case RunRight(leftRow, lkey) =>
              val (nextB, rkey) = rightKeyTrans.f(rstate, rhead)
              val comparator = Slice.rowComparatorFor(lkey, rkey) { s => s.columns.keys.toList.sorted }               

              val nextState = buildFilters(comparator, leftRow, lhead.size, stepleq, 
                                                       0, rhead.size, new mutable.BitSet(), RightSpan)

              continue(nextState, comparator, lstate, lkey, nextB, rkey, leftWriteState, rightWriteState)
            
            case RunLeft(rightRow, rkey) =>
              val (nextA, lkey) = leftKeyTrans.f(lstate, lhead)
              val comparator = Slice.rowComparatorFor(lkey, rkey) { s => s.columns.keys.toList.sorted }

              val nextState = buildFilters(comparator, 0, lhead.size, new mutable.BitSet(), 
                                                       rightRow, rhead.size, stepreq, LeftSpan)

              continue(nextState, comparator, nextA, lkey, rstate, rkey, leftWriteState, rightWriteState)
          }
        }
        
        left.uncons flatMap {
          case Some((lhead, ltail)) =>
            right.uncons.flatMap {
              case Some((rhead, rtail)) =>
                val (lstate, lkey) = leftKeyTrans(lhead)
                val stepResult  = step(FindEqualAdvancingRight(0, lkey), 
                                       lhead, ltail, new mutable.BitSet(),
                                       rhead, rtail, new mutable.BitSet(),
                                       lstate, rightKeyTrans.initial, 
                                       leftWriteState, rightWriteState)

                for {
                  writeStates <- stepResult
                  _      <- M.point(db.close())
                } yield {
                  (
                    loadTable(dbFile, sortMergeEngine, writeStates._1.indices, SortAscending, "left"),
                    loadTable(dbFile, sortMergeEngine, writeStates._2.indices, SortAscending, "right")
                  )
                }

              case None =>
                (Table.empty, Table.empty).point[M]
            }

          case None =>
            (Table.empty, Table.empty).point[M]
        }
      }

      val dbFile = new File(newScratchDir(), "alignSpace")
      val backingDb = DBMaker.openFile(dbFile.getCanonicalPath).make()
      val sourceTrans = composeSliceTransform(Leaf(Source))

      // We need some id that can be used to memoize then load table for each side.
      val leftWriteState = JDBMState(Map(), 0)
      val rightWriteState = JDBMState(Map(), 0)

      writeStreams(dbFile, backingDb, 
                   sourceLeft.slices, composeSliceTransform(alignOnL), 
                   sourceRight.slices, composeSliceTransform(alignOnR), 
                   leftWriteState, rightWriteState)
    }

    def writeTables(db: DB, slices: StreamT[M, Slice], valueTrans: SliceTransform1[_], keyTrans: Seq[SliceTransform1[_]], sortOrder: DesiredSortOrder): M[IndexMap] = {
      def write0(slices: StreamT[M, Slice], state: WriteState): M[IndexMap] = {
        slices.uncons flatMap {
          case Some((slice, tail)) => 
            writeSlice(db, slice, state, sortOrder) flatMap { write0(tail, _) }

          case None => 
            M.point {
              db.close() // No more slices, close out the JDBM database
              state.jdbmState.indices
            }
        }
      }

      write0(slices, WriteState(JDBMState.empty, valueTrans, keyTrans))
    }

    protected def writeSlice(db: DB, slice: Slice, state: WriteState, sortOrder: DesiredSortOrder, source: String = ""): M[WriteState] = {
      val WriteState(jdbmState, valueTrans, keyTrans) = state

      val (valueTrans0, vslice) = valueTrans.advance(slice)
      val (vColumnRefs, vColumns) = vslice.columns.toList.sortBy(_._1).unzip
      val dataRowFormat = RowFormat.forValues(vColumnRefs)
      val dataColumnEncoder = dataRowFormat.ColumnEncoder(vColumns)

      def storeTransformed(jdbmState: JDBMState, transforms: Seq[(SliceTransform1[_], Int)], updatedTransforms: List[SliceTransform1[_]]): M[(JDBMState, List[SliceTransform1[_]])] = transforms match {
        case (keyTransform, i) :: tail => 
          val (nextKeyTransform, kslice) = keyTransform.advance(slice)
          val (keyColumnRefs, keyColumns) = kslice.columns.toList.sortBy(_._1).unzip
          assert (keyColumnRefs.size >= 1)

          val keyRowFormat = RowFormat.forValues(keyColumnRefs)
          val keyColumnEncoder = keyRowFormat.ColumnEncoder(keyColumns)
          val keyComparator = SortingKeyComparator(keyRowFormat, sortOrder.isAscending)

          writeRawSlices(db, kslice, keyColumnRefs, keyColumnEncoder, keyComparator,
                         vslice, vColumnRefs,   dataColumnEncoder, 
                         i.toString, jdbmState) flatMap { newJdbmState =>
            storeTransformed(newJdbmState, tail, nextKeyTransform +: updatedTransforms)
          }

        case Nil => 
          M.point((jdbmState, updatedTransforms))
      }

      storeTransformed(jdbmState, keyTrans.zipWithIndex, Nil) map {
        case (jdbmState0, keyTrans0) => 
          WriteState(jdbmState0, valueTrans0, keyTrans0)
      }
    }

    protected def writeAlignedSlices(db: DB, kslice: Slice, vslice: Slice, jdbmState: JDBMState, indexNamePrefix: String, sortOrder: DesiredSortOrder) = {
      //println("Emitting slice for " + indexNamePrefix)
      //println(vslice)

      val (vColumnRefs, vColumns) = vslice.columns.toList.sortBy(_._1).unzip
      val dataRowFormat = RowFormat.forValues(vColumnRefs)
      val dataColumnEncoder = dataRowFormat.ColumnEncoder(vColumns)

      val (keyColumnRefs, keyColumns) = kslice.columns.toList.sortBy(_._1).unzip
      val keyRowFormat = RowFormat.forValues(keyColumnRefs)
      val keyColumnEncoder = keyRowFormat.ColumnEncoder(keyColumns)
      val keyComparator = SortingKeyComparator(keyRowFormat, sortOrder.isAscending)

      writeRawSlices(db, kslice, keyColumnRefs, keyColumnEncoder, keyComparator,
                         vslice, vColumnRefs, dataColumnEncoder, 
                         indexNamePrefix, jdbmState)  
    }

    protected def writeRawSlices(db: DB,
                             kslice: Slice, krefs: List[ColumnRef], kEncoder: ColumnEncoder, keyComparator: SortingKeyComparator,
                             vslice: Slice, vrefs: List[ColumnRef], vEncoder: ColumnEncoder,
                             indexNamePrefix: String, jdbmState: JDBMState): M[JDBMState] = M.point {
      // Iterate over the slice, storing each row
      @tailrec
      // FIXME: This may not actually be tail recursive!
      // FIXME: Determine whether undefined sort keys are valid
      def storeRow(storage: IndexStore, row: Int, insertCount: Long): Long = {
        if (row < vslice.size) {
          if (vslice.isDefinedAt(row) || kslice.isDefinedAt(row)) {
            storage.put(kEncoder.encodeFromRow(row), vEncoder.encodeFromRow(row))

            if (insertCount % jdbmCommitInterval == 0 && insertCount > 0) db.commit()
            storeRow(storage, row + 1, insertCount + 1)
          } else {
            storeRow(storage, row + 1, insertCount)
          }
        } else {
          insertCount
        }
      }

      val indexMapKey = IndexKey(indexNamePrefix, krefs, vrefs)

      val (index, newIndices) = jdbmState.indices.get(indexMapKey) map { sliceIndex => (sliceIndex, jdbmState.indices) } getOrElse {
        val indexName = indexMapKey.name
        val newSliceIndex = SliceIndex(indexName,
                                  db.createTreeMap(indexName, keyComparator, null, null), // nulls indicate to use default serialization for Array[Byte] 
                                  keyComparator,
                                  krefs.toArray,
                                  vrefs.toArray)

        (newSliceIndex, jdbmState.indices + (indexMapKey -> newSliceIndex))
      }

      val newInsertCount = storeRow(index.storage, 0, jdbmState.insertCount) 

      JDBMState(newIndices, newInsertCount)
    }

    def loadTable(dbFile: File, mergeEngine: MergeEngine[SortingKey, SortBlockData], indices: IndexMap, sortOrder: DesiredSortOrder, notes: String = ""): Table = {
      //println("Losding using indices: " + indices.keys.map(_.name).mkString("\n  ", "\n  ", "\n"))
      import mergeEngine._

      // Map the distinct indices into SortProjections/Cells, then merge them
      def cellsMs: Stream[M[Option[CellState]]] = indices.values.toStream.zipWithIndex map {
        case (SliceIndex(name, _, _, keyColumns, valColumns), index) => 
          val sortProjection = new JDBMRawSortProjection(dbFile, name, keyColumns, valColumns)
          val succ: Option[SortingKey] => M[Option[SortBlockData]] = (key: Option[SortingKey]) => M.point(sortProjection.getBlockAfter(key))
          
          succ(None) map { 
            _ map { nextBlock => 
              CellState(index, nextBlock.maxKey, nextBlock.data, (k: SortingKey) => succ(Some(k))) 
            }
          }
      }

      val head = StreamT.Skip(
        StreamT.wrapEffect(
          for (cellOptions <- cellsMs.sequence) yield {
            mergeProjections(cellOptions.flatMap(a => a), sortOrder.isAscending) { slice => 
              // only need to compare on the group keys (0th element of resulting table) between projections
              slice.columns.keys.collect({ case ref @ ColumnRef(JPath(JPathIndex(0), _ @ _*), _) => ref}).toList.sorted
            }
          }
        )
      )
      
      Table(StreamT(M.point(head))).transform(TransSpec1.DerefArray1)
    }
  }

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import Table._
    import SliceTransform._
    import trans._

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

      def cellsM(projections: Map[ProjectionDescriptor, Set[ColumnDescriptor]]): Stream[M[Option[CellState]]] = {
        for (((desc, cols), i) <- projections.toStream.zipWithIndex) yield {
          val succ: Option[Key] => M[Option[BD]] = (key: Option[Key]) => storage.projection(desc) map {
            case (projection, release) => 
              val result = projection.getBlockAfter(key, cols)  
              release.release.unsafePerformIO
              result
          }

          succ(None) map { 
            _ map { nextBlock => CellState(i, nextBlock.maxKey, nextBlock.data, (k: Key) => succ(Some(k))) }
          }
        }
      }

      val head = StreamT.Skip(
        StreamT.wrapEffect(
          for {
            paths               <- pathsM
            coveringProjections <- (paths map { path => loadable(metadataView, path, JPath.Identity, tpe) }).sequence map { _.flatten }
            cellOptions         <- cellsM(minimalCover(tpe, coveringProjections)).sequence
          } yield {
            mergeProjections(cellOptions.flatMap(a => a), false) { slice => 
              //todo: How do we actually determine the correct function for retrieving the key?
              slice.columns.keys.filter( { case ColumnRef(selector, ctype) => selector.nodes.startsWith(JPathField("key") :: Nil) }).toList.sorted
            }
          }
        )
      )

      //TODO: it turns out load doesn't actualln need to return a table in M
      M.point(Table(StreamT(M.point(head))))
    }

    /**
     * Sorts the KV table by ascending or descending order of a transformation
     * applied to the rows.
     */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = true): M[Table] = groupByN(Seq(sortKey), Leaf(Source), sortOrder, unique).map {
      _.headOption getOrElse this // If we start with an empty table, we always end with an empty table
    }

    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = true): M[Seq[Table]] = {
      writeSorted(groupKeys, valueSpec, sortOrder, unique) map {
        case (dbFile, indices) => 
          indices.groupBy(_._1.streamId).values.toStream.map(loadTable(dbFile, sortMergeEngine, _, sortOrder))
      }
    }

    protected def writeSorted(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = true): M[(File, IndexMap)] = {
      import sortMergeEngine._

      // Open a JDBM3 DB for use in sorting under a temp directory
      val dbFile = new File(newScratchDir(), "writeSortedSpace")

      val (sourceTrans0, keyTrans0, valueTrans0) = if (unique) {
        (
          Scan(
            WrapArray(Leaf(Source)), 
            new CScanner {
              type A = Long
              val init = 0l
              def scan(a: Long, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
                val globalIdColumn = new RangeColumn(range) with LongColumn { def apply(row: Int) = a + row }
                (a + range.end + 1, cols + (ColumnRef(JPath(JPathIndex(1)), CLong) -> globalIdColumn))
              }
            }
          ),
          groupKeys map { kt => 
            ObjectConcat(WrapObject(deepMap(kt) { case Leaf(_) => TransSpec1.DerefArray0 }, "0"), WrapObject(TransSpec1.DerefArray1, "1")) 
          },
          deepMap(valueSpec) { case Leaf(_) => TransSpec1.DerefArray0 }
        )
      } else {
        (Leaf(Source), groupKeys, valueSpec)
      }

      for {
        indices <-  writeTables(
                      DBMaker.openFile(dbFile.getCanonicalPath).make(), 
                      this.transform(sourceTrans0).slices,
                      composeSliceTransform(valueTrans0),
                      keyTrans0 map composeSliceTransform,
                      sortOrder)
      } yield (dbFile, indices)
    }

    /**
     * In order to call partitionMerge, the table must be sorted according to 
     * the values specified by the partitionBy transspec.
     */
    def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table] = {
      @tailrec
      def findEnd(index: Int, size: Int, step: Int, compare: Int => Ordering): Int = {
        if (index < size) {
          compare(index) match {
            case GT =>
              if (step <= 1) index else findEnd(index - (step / 2), size, step / 2, compare)

            case EQ => 
              findEnd(index + step, size, step, compare)

            case LT =>
              sys.error("Inputs to partitionMerge not sorted.")
          }
        } else {
          size
        }
      }

      def subTable(comparatorGen: Slice => (Int => Ordering), slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT.wrapEffect {
        slices.uncons map {
          case Some((head, tail)) =>
            val headComparator = comparatorGen(head)
            val spanEnd = findEnd(0, head.size, head.size - 1, headComparator)
            if (spanEnd < head.size) {
              val (prefix, _) = head.split(spanEnd) 
              prefix :: StreamT.empty[M, Slice]
            } else {
              head :: subTable(comparatorGen, tail)
            }
            
          case None =>
            StreamT.empty[M, Slice]
        }
      }

      def dropAndSplit(comparatorGen: Slice => (Int => Ordering), slices: StreamT[M, Slice]): StreamT[M, Slice] = StreamT.wrapEffect {
        slices.uncons map {
          case Some((head, tail)) =>
            val headComparator = comparatorGen(head)
            val spanEnd = findEnd(0, head.size, head.size - 1, headComparator)
            if (spanEnd < head.size) {
              val (_, suffix) = head.split(spanEnd) 
              stepPartition(suffix, tail)
            } else {
              dropAndSplit(comparatorGen, tail)
            }
            
          case None =>
            StreamT.empty[M, Slice]
        }
      }

      def stepPartition(head: Slice, tail: StreamT[M, Slice]): StreamT[M, Slice] = {
        val comparatorGen = (s: Slice) => {
          val rowComparator = Slice.rowComparatorFor(head, s) {
            (s0: Slice) => s0.columns.keys.collect({ case ref @ ColumnRef(JPath(JPathField("0"), _ @ _*), _) => ref }).toList.sorted
          }

          (i: Int) => rowComparator.compare(0, i)
        }

        val groupedM = f(Table(subTable(comparatorGen, head :: tail)).transform(DerefObjectStatic(Leaf(Source), JPathField("1"))))
        val groupedStream: StreamT[M, Slice] = StreamT.wrapEffect(groupedM.map(_.slices))

        groupedStream ++ dropAndSplit(comparatorGen, head :: tail)
      }

      val keyTrans = ObjectConcat(
        WrapObject(partitionBy, "0"),
        WrapObject(Leaf(Source), "1")
      )

      this.transform(keyTrans).slices.uncons map {
        case Some((head, tail)) =>
          Table(stepPartition(head, tail))
        case None =>
          Table.empty
      }
    }
  }
} 

object BlockStoreColumnarTableModule {
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
  protected def loadable[M[+_]: Monad](metadataView: StorageMetadata[M], path: Path, prefix: JPath, jtpe: JType): M[Set[ProjectionDescriptor]] = {
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
}

// vim: set ts=4 sw=4 et:
