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

import iterable._
import table._
import com.precog.common._ 
import com.precog.util._ 
import com.precog.util.Bijection._

import org.apache.jdbm._

import org.joda.time.DateTime

import java.io._
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.SortedMap
import Bijection._

import com.weiglewilczek.slf4s.Logger
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import scalaz.{Ordering => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.bifunctor
import scalaz.syntax.show._
import scalaz.Scalaz._
import IterateeT._

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

object JDBMProjection {
  private[jdbm3] type IndexTree = SortedMap[Identities,Array[Byte]]

  final val DEFAULT_SLICE_SIZE = 10000
  final val INDEX_SUBDIR = "jdbm"

  def isJDBMProjection(baseDir: File) = (new File(baseDir, INDEX_SUBDIR)).isDirectory
}

abstract class JDBMProjection (val baseDir: File, val descriptor: ProjectionDescriptor, sliceSize: Int = JDBMProjection.DEFAULT_SLICE_SIZE) extends BlockProjectionLike[Identities, Slice] {
  import JDBMProjection._

  val logger = Logger("col:" + descriptor.shows)
  logger.debug("Opening column index files for projection " + descriptor.shows + " at " + baseDir)

  override def toString = "JDBMProjection(" + descriptor.columns + ")"

  private[this] final val treeMapName = "byIdentityMap"

  private[this] final val indexDir = new File(baseDir, INDEX_SUBDIR)

  indexDir.mkdirs()

  implicit val keyOrder = IdentitiesOrder

  protected lazy val idIndexFile: DB = try {
    logger.debug("Opening index file for " + toString)
    DBMaker.openFile((new File(indexDir, "byIdentity")).getCanonicalPath).make()
  } catch {
    case t: Throwable => logger.error("Error on DB open", t); throw t
  }
  
  protected lazy val treeMap: IndexTree = {
    val treeMap: IndexTree = idIndexFile.getTreeMap(treeMapName)
    if (treeMap == null) {
      logger.debug("Creating new projection store")
      val ret: IndexTree = idIndexFile.createTreeMap(treeMapName, AscendingIdentitiesComparator, IdentitiesSerializer(descriptor.identities), null)
      logger.debug("Created projection store")
      ret
    } else {
      logger.debug("Opening existing projection store")
      treeMap
    }
  }

  def close() = {
    logger.info("Closing column index files")
    idIndexFile.commit()
    idIndexFile.close()
    logger.debug("Closed column index files")
  }

  def insert(ids : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO {
    logger.trace("Inserting %s => %s".format(ids, v))
    val codec = new ColumnCodec()
    treeMap.put(ids, codec.encode(v))

    if (shouldSync) {
      idIndexFile.commit()
    }
  }

  def allRecords(expiresAt: Long): IterableDataset[Seq[CValue]] = new IterableDataset(descriptor.identities, new Iterable[(Identities,Seq[CValue])] {
    private val codec = ColumnCodec.readOnly
    def iterator = treeMap.entrySet.iterator.asScala.map { case kvEntry => (kvEntry.getKey, codec.decodeToCValues(kvEntry.getValue)) }
  })

  // Compute the successor to the provided Identities. Assumes that we would never use a VectorCase() for Identities
  private def identitiesAfter(id: Identities) = VectorCase((id.init :+ (id.last + 1)): _*)

  def getBlockAfter(id: Option[Identities], desiredColumns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Identities,Slice]] = {
    import TableModule.paths._

    try {
      // tailMap semantics are >=, but we want > the IDs if provided
      val constrainedMap = id.map { idKey => treeMap.tailMap(identitiesAfter(idKey)) }.getOrElse(treeMap)

      constrainedMap.lastKey() // Will throw an exception if the map is empty

      var firstKey: Identities = null
      var lastKey: Identities  = null

      val slice = new JDBMSlice[Identities] {
        val source = constrainedMap.entrySet.iterator.asScala
        val requestedSize = DEFAULT_SLICE_SIZE
        val codec = ColumnCodec.readOnly

        import blueeyes.json.{JPathField,JPathIndex}

        val keyColumns = (0 until descriptor.identities).map {
          idx: Int => (ColumnRef(JPath(Key :: JPathIndex(idx) :: Nil), CLong), ArrayLongColumn.empty(sliceSize)) 
        }.toArray.asInstanceOf[Array[(ColumnRef,ArrayColumn[_])]]

        val valColumns = descriptor.columns.map {
          case ColumnDescriptor(_, selector, ctpe, _) => JDBMSlice.columnFor(JPath(Value), sliceSize)(ColumnRef(selector, ctpe))
        }.toArray.asInstanceOf[Array[(ColumnRef,ArrayColumn[_])]]

        def loadRowFromKey(row: Int, rowKey: Identities) {
          if (row == 0) { firstKey = rowKey }
          lastKey = rowKey

          var i = 0
          
          while (i < descriptor.identities) {
            keyColumns(i)._2.asInstanceOf[ArrayLongColumn].update(row, rowKey(i))
            i += 1
          }
        }

        val desiredRefs: Set[ColumnRef] = desiredColumns.map { case ColumnDescriptor(_, selector, tpe, _) => ColumnRef(JPath(Value) \ selector, tpe) }

        override val columns = (keyColumns ++ valColumns.filter { case (ref, _) => desiredRefs.contains(ref) }).toMap
      }

      Some(BlockProjectionData[Identities,Slice](firstKey, lastKey, slice))
    } catch {
      case e: java.util.NoSuchElementException => None
    }
  }
}

