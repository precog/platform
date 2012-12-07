package com.precog.yggdrasil
package table

import com.precog.common.json._
import com.precog.yggdrasil.util._

import com.precog.util.{BitSet, BitSetUtil, IOUtils, Loop}
import com.precog.util.BitSetUtil.Implicits._

import scala.collection.mutable
import scala.annotation.tailrec
import scala.util.Random

import scalaz._
import scalaz.syntax.monad._

trait SamplableTableModule[M[+_]]
    extends TableModule[M] {
  import TableModule._

  type Table <: SamplableTable
  
  trait SamplableTable extends TableLike { self: Table =>
    def sample(sampleSize: Int, numOfSamples: Int): M[Set[Table]]
  }
}


trait SamplableColumnarTableModule[M[+_]]
    extends SamplableTableModule[M] { self: ColumnarTableModule[M] =>

  def rng: Random = scala.util.Random

  type Table <: ColumnarTable with SamplableTable

  trait SamplableColumnarTable extends SamplableTable { self: Table =>

    /**
     * A one-pass algorithm for sampling. This runs in time O(H_n*m^2 + n) =
     * O(m^2 lg n + n), so it is not super optimal. Another good option is to
     * try Alissa's approach; keep 2 buffers of size m. Load one up fully,
     * shuffle, then replace, but we aren't 100% sure it is uniform and
     * independent.
     *
     * Of course, the hope is that this will not be used once we get efficient
     * sampling in that runs in O(m lg n) time.
     */
    def sample(sampleSize: Int, numOfSamples: Int): M[Set[Table]] = {
      def build(prevInserters: Option[Array[RowInserter]], n: Int, slices: StreamT[M, Slice]): M[Set[Table]] = {
        slices.uncons flatMap {
          case Some((slice, slices)) =>
            val inserters = prevInserters map { _ map (_.withSource(slice)) } getOrElse {
              Array.fill(numOfSamples)(RowInserter(sampleSize, slice))
            }

            @tailrec
            def loop(i: Int): Int = if (i < slice.size) {
              var j = 0
              while (j < inserters.length) {
                var k = rng.nextInt(n + i + 1)
                if (k < sampleSize) {
                  inserters(j).insert(src=i, dest=k)
                }
                j += 1
              }
              loop(i + 1)
            } else i

            val len = loop(0)
            build(Some(inserters), n + len, slices)

          case None =>
            M.point {
              val len = n min sampleSize
              prevInserters map { _ map (_.toSlice(len)) } map { slices =>
                slices.map { slice =>
                  Table(slice :: StreamT.empty[M, Slice], ExactSize(len)).paged(yggConfig.maxSliceSize)
                }.toSet
              } getOrElse {
                Seq.fill(sampleSize)(Table(StreamT.empty[M, Slice], ExactSize(0))).toSet
              }
            }
        }
      }

      build(None, 0, slices)
    }
  }

  private case class RowInserter(size: Int, slice: Slice, cols: mutable.Map[ColumnRef, ArrayColumn[_]] = mutable.Map.empty) {
    import RowInserter._

    def toSlice(maxSize: Int): Slice = Slice(cols.toMap, size min maxSize)

    val ops: Array[ColumnOps] = slice.columns.map(colOpsFor)(collection.breakOut)

    def insert(src: Int, dest: Int) {
      var k = 0
      while (k < ops.length) {
        val col = ops(k)
        var i = size - 1
        while (i > dest) {
          col.move(i - 1, i)
          i -= 1
        }
        col.insert(src, dest)

        k += 1
      }
    }

    def withSource(slice: Slice): RowInserter = RowInserter(size, slice, cols)

    // Creates array columns on demand.
    private def getOrCreateCol(ref: ColumnRef): ArrayColumn[_] = {
      cols.getOrElseUpdate(ref, ref.ctype match {
        case CBoolean => ArrayBoolColumn.empty()
        case CLong => ArrayLongColumn.empty(size)
        case CDouble => ArrayDoubleColumn.empty(size)
        case CNum => ArrayNumColumn.empty(size)
        case CString => ArrayStrColumn.empty(size)
        case CDate => ArrayDateColumn.empty(size)
        case CArrayType(elemType) => ArrayHomogeneousArrayColumn.empty(size)(elemType)
        case CNull => MutableNullColumn.empty()
        case CEmptyObject => MutableEmptyObjectColumn.empty()
        case CEmptyArray => MutableEmptyArrayColumn.empty()
        case CUndefined => sys.error("this shound't exist")
      })
    }

    private def colOpsFor: ((ColumnRef, Column)) => ColumnOps = { case (ref, col) =>
      (col, getOrCreateCol(ref)) match {
        case (src: BoolColumn, dest: ArrayBoolColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src: LongColumn, dest: ArrayLongColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src: DoubleColumn, dest: ArrayDoubleColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src: NumColumn, dest: ArrayNumColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src: StrColumn, dest: ArrayStrColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src: DateColumn, dest: ArrayDateColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src: NullColumn, dest: MutableNullColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, true)
            def unsafeMove(from: Int, to: Int) = dest.update(to, true)
          }
        case (src: EmptyObjectColumn, dest: MutableEmptyObjectColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, true)
            def unsafeMove(from: Int, to: Int) = dest.update(to, true)
          }
        case (src: EmptyArrayColumn, dest: MutableEmptyArrayColumn) =>
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, true)
            def unsafeMove(from: Int, to: Int) = dest.update(to, true)
          }
        case (src: HomogeneousArrayColumn[a], dest0: ArrayHomogeneousArrayColumn[_]) if src.tpe == dest0.tpe =>
          val dest = dest0.asInstanceOf[ArrayHomogeneousArrayColumn[a]]
          new ColumnOps(src, dest) {
            def unsafeInsert(srcRow: Int, destRow: Int) = dest.update(destRow, src(srcRow))
            def unsafeMove(from: Int, to: Int) = dest.update(to, dest(from))
          }
        case (src, dest) =>
          sys.error("Slice lied about column type. Expected %s, but found %s." format (ref.ctype, src.tpe))
      }
    }
  }

  private object RowInserter {
    abstract class ColumnOps(src: Column, dest: ArrayColumn[_]) {
      protected def unsafeInsert(srcRow: Int, destRow: Int): Unit
      protected def unsafeMove(fromRow: Int, toRow: Int): Unit

      final def insert(srcRow: Int, destRow: Int) {
        if (src.isDefinedAt(srcRow)) {
          unsafeInsert(srcRow, destRow)
        } else {
          dest.defined.clear(destRow)
        }
      }

      final def move(from: Int, to: Int) {
        if (dest.isDefinedAt(from)) {
          unsafeMove(from, to)
        } else {
          dest.defined.clear(to)
        }
      }
    }
  }

}

