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

import table._
import com.precog.common._ 
import com.precog.common.json._ 
import com.precog.util._ 
import com.precog.util.Bijection._

import org.apache.jdbm._

import org.joda.time.DateTime

import java.io._
import java.nio.ByteBuffer
import java.util.Map.Entry
import java.util.SortedMap
import Bijection._

import org.slf4j._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import scalaz.{Ordering => _, _}
import scalaz.effect._
import scalaz.syntax.plus._
import scalaz.syntax.monad._
import scalaz.syntax.applicativePlus._
import scalaz.syntax.bifunctor
import scalaz.syntax.show._
import scalaz.Scalaz._

import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.JsonParser
import blueeyes.json.Printer
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

object JDBMProjection {
  private[jdbm3] type IndexTree = SortedMap[Array[Byte],Array[Byte]]

  final val INDEX_SUBDIR = "jdbm"
  final val MAX_SPINS = 20 // FIXME: This is related to the JDBM ConcurrentMod exception, and should be removed when that's cleaned up
    
  def isJDBMProjection(baseDir: File) = (new File(baseDir, INDEX_SUBDIR)).isDirectory
}

// FIXME: Again, related to JDBM concurent mod exception
class VicciniException(message: String) extends java.io.IOException("Inconceivable! " + message)

abstract class JDBMProjection (val baseDir: File, val descriptor: ProjectionDescriptor, sliceSize: Int) extends BlockProjectionLike[Array[Byte], Slice] { projection =>
  import TransSpecModule.paths._
  import JDBMProjection._

  val logger = LoggerFactory.getLogger("com.precog.yggdrasil.jdbm3.JDBMProjection")

  val keyColRefs = Array.tabulate(descriptor.identities) { i => ColumnRef(CPath(Key :: CPathIndex(i) :: Nil), CLong) }

  val keyFormat: IdentitiesRowFormat = RowFormat.IdentitiesRowFormatV1(keyColRefs)

  val rowFormat: RowFormat = RowFormat.ValueRowFormatV1(descriptor.columns map {
    case ColumnDescriptor(_, cPath, cType, _) => ColumnRef(cPath, cType)
  })

  override def toString = "JDBMProjection(" + descriptor.shows + ")"

  private[this] final val treeMapName = "byIdentityMap"

  private[this] final val indexDir = new File(baseDir, INDEX_SUBDIR)

  indexDir.mkdirs()

  implicit val keyOrder = IdentitiesOrder

  def setMDC() {
    MDC.put("projection", descriptor.shows)
  }
  
  // TODO: Make this safe
  //  def size(): Long = idIndexFile.collectionSize(treeMap)

  protected lazy val idIndexFile: DB = try {
    logger.debug("Opening index file for " + toString + " from " + baseDir)
    DBMaker.openFile((new File(indexDir, "byIdentity")).getCanonicalPath).make()
  } catch {
    case t: Throwable => logger.error("Error on DB open", t); throw t
  }
  
  protected lazy val treeMap: IndexTree = {
    val treeMap: IndexTree = idIndexFile.getTreeMap(treeMapName)
    if (treeMap == null) {
      logger.debug("Creating new projection store")
      val ret: IndexTree = idIndexFile.createTreeMap(treeMapName, SortingKeyComparator(keyFormat, true), ByteArraySerializer, ByteArraySerializer)
      logger.debug("Created projection store")
      ret
    } else {
      logger.debug("Opening existing projection store")
      treeMap
    }
  }

  def close(): IO[Unit] = IO {
    setMDC()
    logger.trace("Closing column index files")
    idIndexFile.commit()
    idIndexFile.close()
    logger.trace("Closed column index files")
  }.ensuring {
    IO { MDC.clear() }
  }

  def commit(): IO[Unit] = IO {
    setMDC()
    logger.trace("Committing column index files")
    idIndexFile.commit()
    logger.trace("Committed column index files")
  } ensuring {
    IO { MDC.clear() }
  }

  def insert(ids : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace("Inserting %s => %s".format(ids.mkString("[", ", ", "]"), v))
    }

    treeMap.put(keyFormat.encodeIdentities(ids), rowFormat.encode(v.toList))

    if (shouldSync) {
      idIndexFile.commit()
    }
  }

  def getBlockAfter(id: Option[Array[Byte]], desiredColumns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Array[Byte],Slice]] = {
    setMDC()
    if (idIndexFile.isClosed()) {
      sys.error("Attempting to retrieve more data from a closed projection")
    }
    
    if (logger.isDebugEnabled) {
      logger.debug("Retrieving key after " + id.map(_.mkString("[", ", ", "]")))
    }

    val startTime = System.currentTimeMillis

    try {
      // tailMap semantics are >=, but we want > the IDs if provided
      val constrainedMap = id.map { idKey => treeMap.tailMap(idKey) } getOrElse treeMap
      
      val iteratorSetup = () => {
        val rawIterator = constrainedMap.entrySet.iterator.asScala
        // Since our key to retrieve after was the last key we retrieved, we know it exists,
        // so we can safely discard it
        if (id.isDefined && rawIterator.hasNext) rawIterator.next();
        rawIterator
      }

      // FIXME: this is brokenness in JDBM somewhere      
      val iterator = {
        var initial: Iterator[java.util.Map.Entry[Array[Byte],Array[Byte]]] = null
        var tries = 0
        while (tries < JDBMProjection.MAX_SPINS && initial == null) {
          try {
            initial = iteratorSetup()
          } catch {
            case t: Throwable => logger.warn("Failure on load iterator initialization")
          }
          tries += 1
        }
        if (initial == null) {
          throw new VicciniException("Initial drop failed with too many concurrent mods.")
        } else {
          initial
        }
      }

      if (iterator.isEmpty) {
        None 
      } else {
        val keyCols = Array.fill(descriptor.identities) { ArrayLongColumn.empty(sliceSize) }
        val valColumns = rowFormat.columnRefs.map(JDBMSlice.columnFor(CPath(Value), sliceSize))

        val keyColumnDecoder = keyFormat.ColumnDecoder(keyCols)
        val valColumnDecoder = rowFormat.ColumnDecoder(valColumns.map(_._2)(collection.breakOut))

        val (firstKey, lastKey, rows) = JDBMSlice.load(sliceSize, iteratorSetup, keyColumnDecoder, valColumnDecoder)

        val slice = new Slice {
          private val desiredRefs: Set[ColumnRef] = desiredColumns map { 
            case ColumnDescriptor(_, selector, tpe, _) => ColumnRef(CPath(Value) \ selector, tpe) 
          }
          
          val size = rows
          val columns = keyColRefs.iterator.zip(keyCols.iterator).toMap ++ valColumns.iterator.filter({
            case (ref @ ColumnRef(CPath(Value, _*), _), _) => desiredRefs.contains(ref)
            case _ => true
          })
        }

        Some(BlockProjectionData[Array[Byte],Slice](firstKey, lastKey, slice))
      }
    } catch {
      case e: java.util.NoSuchElementException => None
    } finally {
      if (logger.isDebugEnabled) {
        logger.debug("Block retrieved in %d ms".format(System.currentTimeMillis - startTime))
      }
      MDC.clear()
    }
  }
}

