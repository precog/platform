package com.precog.yggdrasil
package jdbm3

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import java.nio.ByteBuffer
import java.util.SortedMap

import com.precog.util.Bijection._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.serialization.bijections._

import scala.collection.JavaConverters._

import blueeyes.json.JPath

import JDBMProjection._

/**
 * A slice built from a JDBMProjection with a backing array of key/value pairs
 *
 * @param source A source iterator of Map.Entry[Key,Array[Byte]] pairs, positioned at the first element of the slice
 * @param size How many entries to retrieve in this slice
 */
trait JDBMSlice[Key] extends Slice with Logging {
  protected def source: Iterator[java.util.Map.Entry[Key,Array[Byte]]]
  protected def requestedSize: Int

  protected def keyColumns: Array[(ColumnRef, ArrayColumn[_])]
  protected def valColumns: Array[(ColumnRef, ArrayColumn[_])]

  def columnDecoder: ColumnDecoder

  // This method is responsible for loading the data from the key at the given row,
  // most likely into one or more of the key columns defined above
  protected def loadRowFromKey(row: Int, key: Key): Unit

  private var row = 0

  protected def load() {
    source.take(requestedSize).foreach {
      entry => {
        loadRowFromKey(row, entry.getKey)
        columnDecoder.decodeToRow(row, entry.getValue)
        row += 1
      }
    }
  }

  def size = row

  def columns: Map[ColumnRef, Column] = keyColumns.++(valColumns)(collection.breakOut)
}

object JDBMSlice {
  def columnFor(prefix: JPath, sliceSize: Int)(ref: ColumnRef): (ColumnRef, ArrayColumn[_]) =
    (ref.copy(selector = (prefix \ ref.selector)), (ref.ctype match {
      case CString      => ArrayStrColumn.empty(sliceSize)
      case CBoolean     => ArrayBoolColumn.empty()
      case CLong        => ArrayLongColumn.empty(sliceSize)
      case CDouble      => ArrayDoubleColumn.empty(sliceSize)
      case CNum         => ArrayNumColumn.empty(sliceSize)
      case CDate        => ArrayDateColumn.empty(sliceSize)
      case CNull        => MutableNullColumn.empty()
      case CEmptyObject => MutableEmptyObjectColumn.empty()
      case CEmptyArray  => MutableEmptyArrayColumn.empty()
      case CUndefined   => sys.error("CUndefined cannot be serialized")
    }))
}

