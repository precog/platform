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

import com.precog.common.Path
import com.precog.yggdrasil.table._

import blueeyes.json.{JPath,JPathIndex}
import org.apache.jdbm._

import scalaz.effect.IO

import java.io.File
import java.util.SortedMap

import scala.collection.JavaConverters._

/**
 * A Projection wrapping a raw JDBM TreeMap index used for sorting. It's assumed that
 * the index has been created and filled prior to creating this wrapper.
 */
abstract class JDBMRawSortProjection private[yggdrasil] (dbFile: File, indexName: String, idCount: Int, keyRefs: Seq[ColumnRef], valRefs: Seq[ColumnRef], sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[SortingKey,Slice] {
  // These should not actually be used in sorting
  def descriptor: ProjectionDescriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = sys.error("Insertion on sort projections is unsupported")

  def getBlockAfter(id: Option[SortingKey], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[SortingKey,Slice]] = try {
    import TableModule.paths._

    // TODO: Make this far, far less ugly
    if (columns.size > 0) {
      throw new IllegalArgumentException("JDBM Sort Projections may not be constrained by column descriptor")
    }

    val DB = DBMaker.openFile(dbFile.getCanonicalPath).make()
    val index: SortedMap[SortingKey,Array[CValue]] = DB.getTreeMap(indexName)

    if (index == null) {
      throw new IllegalArgumentException("No such index in DB: %s:%s".format(dbFile, indexName))
    }

    val constrainedMap = id.map { idKey => index.tailMap(idKey.copy(index = idKey.index + 1)) }.getOrElse(index)
    
    constrainedMap.lastKey() // Will throw an exception if the map is empty

    val slice = new ArrayRowJDBMSlice[SortingKey] {
      val source = constrainedMap.entrySet.iterator.asScala
      val requestedSize = sliceSize

      import blueeyes.json.{JPathField,JPathIndex}

      case class IdentColumn(index: Int) extends LongColumn with BaseColumn {
        def apply(row: Int): Long = backing(row).getKey.ids.apply(index)
      }
      
      def keyColumns: Map[ColumnRef,Column] = (0 until idCount).map {
        idx: Int => ColumnRef(JPath(Key :: JPathIndex(idx) :: Nil), CLong) -> IdentColumn(idx)
      }.toMap

      def sortKeyColumns: Map[ColumnRef,Column] = keyRefs.zipWithIndex.map {
        case (ColumnRef(selector, tpe),index) => 
          columnFor(row => backing(row).getKey.columns, ColumnRef(JPath(SortKey) \ selector, tpe), index)
      }.toMap
      
      def valColumns: Map[ColumnRef,Column] = valRefs.zipWithIndex.map {
        case (ColumnRef(selector, tpe), index) =>
          columnFor(row => backing(row).getValue, ColumnRef(JPath(Value) \ selector, tpe), index)
      }.toMap

      lazy val columns = keyColumns ++ sortKeyColumns ++ valColumns
    }

    DB.close() // creating the slice should have already read contents into memory

    Some(BlockProjectionData[SortingKey,Slice](slice.firstKey, slice.lastKey, slice))
  } catch {
    case  e: java.util.NoSuchElementException => None
  }
}
