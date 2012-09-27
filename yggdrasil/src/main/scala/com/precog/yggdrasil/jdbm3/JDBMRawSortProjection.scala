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
import com.precog.yggdrasil.TableModule._

import blueeyes.json._

import org.apache.jdbm._
import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import scalaz.effect.IO

import java.io.File
import java.util.SortedMap
import java.nio.ByteBuffer

import scala.collection.JavaConverters._

/**
 * A Projection wrapping a raw JDBM TreeMap index used for sorting. It's assumed that
 * the index has been created and filled prior to creating this wrapper.
 */
class JDBMRawSortProjection private[yggdrasil] (dbFile: File, indexName: String, sortKeyRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef], sortOrder: DesiredSortOrder, sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[Array[Byte],Slice] with Logging {

  // These should not actually be used in sorting
  def descriptor: ProjectionDescriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Insertion on sort projections is unsupported")

  def foreach(f : java.util.Map.Entry[Array[Byte], Array[Byte]] => Unit) {
    val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[Array[Byte],Array[Byte]] = DB.getTreeMap(indexName)

    index.entrySet().iterator().asScala.foreach(f)

    DB.close()
  }

  val keyAfterDelta = if (sortOrder.isAscending) 1 else -1

  val rowFormat = RowFormat.forValues(valRefs)
  val keyFormat = RowFormat.forSortingKey(sortKeyRefs)

  def getBlockAfter(id: Option[Array[Byte]], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Array[Byte], Slice]] = {
    // TODO: Make this far, far less ugly
    if (columns.size > 0) {
      throw new IllegalArgumentException("JDBM Sort Projections may not be constrained by column descriptor")
    }

    // At this point we have completed all valid writes, so we open readonly + no locks, allowing for concurrent use of sorted data
    //println("opening: " + dbFile.getCanonicalPath)
    val db = DBMaker.openFile(dbFile.getCanonicalPath).readonly().disableLocking().make()
    try {
      val index: SortedMap[Array[Byte],Array[Byte]] = db.getTreeMap(indexName)

      if (index == null) {
        throw new IllegalArgumentException("No such index in DB: %s:%s".format(dbFile, indexName))
      }

      val constrainedMap = id.map { idKey => index.tailMap(idKey) }.getOrElse(index)
      val rawIterator = constrainedMap.entrySet.iterator.asScala
      if (id.isDefined && rawIterator.hasNext) rawIterator.next(); // TODO Ensure first matches id before drop?

      if (rawIterator.isEmpty) {
        None
      } else {
        val keyColumns = sortKeyRefs.map(JDBMSlice.columnFor(JPath("[0]"), sliceSize))
        val valColumns = valRefs.map(JDBMSlice.columnFor(JPath("[1]"), sliceSize))

        val keyColumnDecoder = keyFormat.ColumnDecoder(keyColumns.map(_._2)(collection.breakOut))
        val valColumnDecoder = rowFormat.ColumnDecoder(valColumns.map(_._2)(collection.breakOut))

        val (firstKey, lastKey, rows) = JDBMSlice.load(sliceSize, rawIterator, keyColumnDecoder, valColumnDecoder)

        val slice = new Slice { 
          val size = rows 
          val columns = keyColumns.toMap ++ valColumns
        }

        Some(BlockProjectionData[Array[Byte],Slice](firstKey, lastKey, slice))
      }
    } finally {
      db.close() // creating the slice should have already read contents into memory
    }
  }
}
