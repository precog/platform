package com.precog.yggdrasil
package leveldb

import com.weiglewilczek.slf4s.Logging

import org.fusesource.leveldbjni.KeyValueChunk
import org.joda.time.DateTime

import java.nio.ByteBuffer

import com.precog.util.Bijection._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization.bijections._

import blueeyes.json.{JPath,JPathField,JPathIndex}

/**
 * A slice that wraps a LevelDB KeyValue Chunk from a given
 * LevelDB Projection.
 */
sealed trait LevelDBSlice extends Slice with Logging {
  protected val chunk: KeyValueChunk
  protected val identCount: Int
  protected val descriptors: Seq[ColumnDescriptor]

  val size = chunk.pairLength

  trait BaseColumn {
    def isDefinedAt(row: Int) = row < size
  }

  case class IdentColumn(index: Int) extends LongColumn with BaseColumn {
    private[this] final val rowWidth = identCount * 8
    def apply(row: Int): Long = chunk.keyData.getLong(row * rowWidth + index * 8)
  }

  protected def keyColumns: Map[ColumnRef, Column] = (0 until identCount).map {
    idx: Int => ColumnRef(JPath(JPathField("key") :: JPathIndex(idx) :: Nil), CLong) -> IdentColumn(idx)
  }.toMap

  protected def valColumns: Seq[(ColumnRef, Column)]

  lazy val columns: Map[ColumnRef, Column] = keyColumns ++ valColumns

  object LNullColumn extends table.NullColumn with BaseColumn
  object LEmptyObjectColumn extends table.EmptyObjectColumn with BaseColumn
  object LEmptyArrayColumn extends table.EmptyArrayColumn with BaseColumn
}

object LevelDBSlice {
  def apply(chunk: KeyValueChunk, identCount: Int, descriptors: Seq[ColumnDescriptor]) = chunk.valueWidth match {
    case w: FixedWidth if descriptors.forall { case ColumnDescriptor(_, _, ctpe, _) => ctpe.format.isFixed } => new FixedLevelDBSlice(chunk, identCount, descriptors)
    case _ => new VariableLevelDBSlice(chunk, identCount, descriptors)
  }

  /**
   * A slice for which all columns are fixed width.
   */
  class FixedLevelDBSlice private[LevelDBSlice] (val chunk: KeyValueChunk, val identCount: Int, val descriptors: Seq[ColumnDescriptor]) extends LevelDBSlice {
    private val offsets: List[Int] = descriptors.foldLeft((0,List[Int]())) { 
      case ((current,offsets), ColumnDescriptor(_, _, ctpe, _)) => (current + ctpe.format.asInstanceOf[FixedWidth].width, current :: offsets)
    }._2.reverse

    private val rowWidth = offsets.sum

    def valColumns: Seq[(ColumnRef, Column)] = descriptors.zip(offsets).map {
      case (ColumnDescriptor(_, selector, ctpe, _),rowOffset) => ColumnRef(JPath(".value") \ selector, ctpe) -> (ctpe match {
        case CBoolean => new BoolColumn with BaseColumn {
          def apply(row: Int): Boolean = chunk.valueData.get(row * rowWidth + rowOffset) != 0
        }

        case  CLong  => new LongColumn with BaseColumn {
          def apply(row: Int): Long = chunk.valueData.getLong(row * rowWidth + rowOffset)
        }

        case CDouble => new DoubleColumn with BaseColumn {
          def apply(row: Int): Double = chunk.valueData.getDouble(row * rowWidth + rowOffset) 
        }

        case CDate => new DateColumn with BaseColumn {
          def apply(row: Int): DateTime = new DateTime(chunk.valueData.getLong(row * rowWidth + rowOffset))
        }

        case CNull => LNullColumn

        case CEmptyObject => LEmptyObjectColumn

        case CEmptyArray => LEmptyArrayColumn

        // CStringFixed will be departing soon, so we're not handling it now

        case invalid => sys.error("Invalid fixed with CType: " + invalid)
      })
    }
  }

  /**
   * A slice for which at least one column is variable width. NOT THREAD SAFE.
   */
  class VariableLevelDBSlice private[LevelDBSlice] (val chunk: KeyValueChunk, val identCount: Int, val descriptors: Seq[ColumnDescriptor]) extends LevelDBSlice {
    private final val columnCount = descriptors.size

    private val columnWidth: Array[Int] = descriptors.map { case ColumnDescriptor(_, _, ctpe, _) => ctpe.format match {
      case LengthEncoded     => -1
      case FixedWidth(width) => width
    }}.toArray

    // We assume for the sake of optimization that traversal is in increasing row index order. Accessing a row
    // prior to the last retrieved row incurs a rescan from the beginning
    private var nextRow = 0
    private var nextRowOffset = 0
    private var computedRow = -1 // The last row for which we've computed offsets/lengths
    private var offset = new Array[Int](columnCount)
    private var length = new Array[Int](columnCount)

    private def computeColumnInfo(row: Int) {
      // Reset if we have to reverse
      if (row < computedRow) {
        nextRow = 0
        nextRowOffset = 0
      }

      while (nextRow < row) {
        // We can skip whole rows since LevelDB does RLE for each one
        nextRowOffset += (chunk.valueData.getInt(nextRowOffset) + 4)
        nextRow += 1
      }

      if (row != computedRow) {
        offset(0) = nextRowOffset + 4

        var index = 0

        while (index < columnCount) {
          if (columnWidth(index) >= 0) {
            length(index) = columnWidth(index)
          } else {
            length(index) = chunk.valueData.getInt(offset(index))
            offset(index) += 4 // Skip the length Int
          }

          val nextOffset = offset(index) + length(index)

          if (index < (columnCount - 1)) {
            offset(index + 1) = nextOffset
          } else {
            nextRow = row + 1
            nextRowOffset = nextOffset
          }

          index += 1
        }
      }
    }

    def valColumns: Seq[(ColumnRef, Column)] = descriptors.zipWithIndex.map {
      case (ColumnDescriptor(_, selector, ctpe, _),index) => ColumnRef(selector, ctpe) -> (ctpe match {
        //// Fixed width types within the var width row
        case CBoolean => new BoolColumn with BaseColumn {
          def apply(row: Int): Boolean = {
            computeColumnInfo(row)
            chunk.valueData.get(offset(index)) != 0.asInstanceOf[Byte]
          }
        }

        case  CLong  => new LongColumn with BaseColumn {
          def apply(row: Int): Long = {
            computeColumnInfo(row)
            chunk.valueData.getLong(offset(index))
          }
        }

        case CDouble => new DoubleColumn with BaseColumn {
          def apply(row: Int): Double = {
            computeColumnInfo(row)
            chunk.valueData.getDouble(offset(index))
          }
        }

        case CDate => new DateColumn with BaseColumn {
          def apply(row: Int): DateTime = {
            computeColumnInfo(row)
            new DateTime(chunk.valueData.getLong(offset(index)))
          }
        }

        case CNull => LNullColumn

        case CEmptyObject => LEmptyObjectColumn

        case CEmptyArray => LEmptyArrayColumn

        // CStringFixed will be departing soon, so we're not handling it now

        //// Variable width types
        case CString => new StrColumn with BaseColumn {
          def apply(row: Int): String = {
            computeColumnInfo(row)
            val work = new Array[Byte](length(index))
            chunk.valueData.get(work, offset(index), length(index))
            work.as[String]
          }
        }

        case CNum => new NumColumn with BaseColumn {
          def apply(row: Int): BigDecimal = {
            computeColumnInfo(row)
            val work = new Array[Byte](length(index))
            chunk.valueData.get(work, offset(index), length(index))
            work.as[BigDecimal]
          }
        }

        case invalid => sys.error("Invalid fixed with CType: " + invalid)
      })
    }
  }
}

// vim: set ts=4 sw=4 et:
