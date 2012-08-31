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
package jdbm3

import com.precog.common.json._
import com.precog.common.Path
import com.precog.yggdrasil.table._

import org.apache.jdbm._
import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import scalaz.effect.IO

import java.io.File
import java.util.SortedMap

import scala.collection.JavaConverters._

/**
 * A Projection wrapping a raw JDBM TreeMap index used for sorting. It's assumed that
 * the index has been created and filled prior to creating this wrapper.
 */
abstract class JDBMRawSortProjection private[yggdrasil] (dbFile: File, indexName: String, idCount: Int, sortKeyRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef], sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[SortingKey,Slice] with Logging { projection =>
  import TableModule.paths._

  // These should not actually be used in sorting
  def descriptor: ProjectionDescriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Insertion on sort projections is unsupported")

  def foreach(f : java.util.Map.Entry[SortingKey,Array[Byte]] => Unit) {
    val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[SortingKey,Array[Byte]] = DB.getTreeMap(indexName)

    index.entrySet().iterator().asScala.foreach(f)

    DB.close()
  }

  private val rowCodec = Codec.RowCodec((valRefs map (_.ctype)).toList)

  def getBlockAfter(id: Option[SortingKey], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[SortingKey,Slice]] = try {
    // TODO: Make this far, far less ugly
    if (columns.size > 0) {
      throw new IllegalArgumentException("JDBM Sort Projections may not be constrained by column descriptor")
    }

    val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[SortingKey,Array[Byte]] = DB.getTreeMap(indexName)

    if (index == null) {
      throw new IllegalArgumentException("No such index in DB: %s:%s".format(dbFile, indexName))
    }

    val constrainedMap = id.map { idKey => index.tailMap(idKey.copy(index = idKey.index + 1)) }.getOrElse(index)
    
    constrainedMap.lastKey() // should throw an exception if the map is empty, but...

    var firstKey: SortingKey = null
    var lastKey: SortingKey  = null

    val slice = new JDBMSlice[SortingKey] {
      def source = constrainedMap.entrySet.iterator.asScala
      def requestedSize = sliceSize
      lazy val idColumns      = (0 until idCount).map { idx => (ColumnRef(CPath(Key :: CPathIndex(idx) :: Nil), CLong), ArrayLongColumn.empty(sliceSize)) }.toArray
      lazy val sortKeyColumns = sortKeyRefs.map(JDBMSlice.columnFor(CPath(SortKey), sliceSize)).toArray

      // lazy val sortKeyColumns = sortKeyRefs map { case ColumnRef(selector, cType) =>
      //   (ColumnRef(CPath(SortKey) \ selector, cType), ColCodec.forCType(cType, sliceSize))
      // }

      lazy val keyColumns = (idColumns ++ sortKeyColumns).asInstanceOf[Array[(ColumnRef,ArrayColumn[_])]]
      lazy val valColumns: Array[(ColumnRef, ColCodec[_])] = valRefs.map { case ColumnRef(selector, cType) =>
        (ColumnRef(CPath(Value) \ selector, cType), ColCodec.forCType(cType, sliceSize))
      }(collection.breakOut)

      val rowCodec = projection.rowCodec

      def loadRowFromKey(row: Int, rowKey: SortingKey) {
        if (row == 0) { firstKey = rowKey }
        lastKey = rowKey

        var i = 0

        while (i < idCount) {
          idColumns(i)._2.update(row, rowKey.ids(i))
          i += 1
        }

        val sortKeyColumnsRaw = ColumnCodec.readOnly.decodeWithRefs(rowKey.columns)

        i = 0
        while (i < sortKeyColumns.length) {
          sortKeyColumnsRaw(i) match {
            case (_, CString(cs))   => sortKeyColumns(i)._2.asInstanceOf[ArrayStrColumn].update(row, cs)
            case (_, CBoolean(cb))  => sortKeyColumns(i)._2.asInstanceOf[ArrayBoolColumn].update(row, cb)
            case (_, CLong(cl))     => sortKeyColumns(i)._2.asInstanceOf[ArrayLongColumn].update(row, cl)
            case (_, CDouble(cd))   => sortKeyColumns(i)._2.asInstanceOf[ArrayDoubleColumn].update(row, cd)
            case (_, CNum(cn))      => sortKeyColumns(i)._2.asInstanceOf[ArrayNumColumn].update(row, cn)
            case (_, CDate(cd))     => sortKeyColumns(i)._2.asInstanceOf[ArrayDateColumn].update(row, cd)
            case (_, CArray(as: IndexedSeq[a], _)) =>
              sortKeyColumns(i)._2.asInstanceOf[ArrayHomogeneousArrayColumn[a]].update(row, as)
            case (_, CNull)         => sortKeyColumns(i)._2.asInstanceOf[MutableNullColumn].update(row, true)
            case (_, CEmptyObject)  => sortKeyColumns(i)._2.asInstanceOf[MutableEmptyObjectColumn].update(row, true)
            case (_, CEmptyArray)   => sortKeyColumns(i)._2.asInstanceOf[MutableEmptyArrayColumn].update(row, true)                      
            case (_, CUndefined)    => // NOOP, array/mutable columns start fully undefined
          }
          i += 1
        }
      }

      load()
    }

    DB.close() // creating the slice should have already read contents into memory

    if (firstKey == null) { // Just guard against an empty slice
      None
    } else {
      Some(BlockProjectionData[SortingKey,Slice](firstKey, lastKey, slice))
    }
  } catch {
    case e: java.util.NoSuchElementException => None
  }
}
