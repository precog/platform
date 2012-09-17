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

import scala.annotation.tailrec
import JDBMProjection._

object JDBMSlice {
  def load[Key](size: Int, source: Iterator[java.util.Map.Entry[Key,Array[Byte]]], keyDecoder: (Int, Key) => Unit, valDecoder: ColumnDecoder): (Key, Key, Int) = {
    var firstKey: Key = null.asInstanceOf[Key]
    var lastKey: Key  = null.asInstanceOf[Key]

    @tailrec
    def consumeRows(source: Iterator[java.util.Map.Entry[Key, Array[Byte]]], row: Int): Int = {
      if (source.hasNext) {
        val entry = source.next
        val rowKey = entry.getKey
        if (row == 0) { firstKey = rowKey }
        lastKey = rowKey

        keyDecoder(row, rowKey)
        valDecoder.decodeToRow(row, entry.getValue)
        consumeRows(source, row + 1)
      } else {
        row
      }
    }
    
    val rows = consumeRows(source.take(size), 0)

    (firstKey, lastKey, rows)
  }

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

