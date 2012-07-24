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
  private[jdbm3] type IndexTree = SortedMap[Identities,Seq[CValue]]

  val DEFAULT_SLICE_SIZE = 10000
}

abstract class JDBMProjection (val baseDir: File, val descriptor: ProjectionDescriptor) extends FullProjectionLike[IterableDataset[Seq[CValue]]] with BlockProjectionLike[Slice] {
  import JDBMProjection._

  val logger = Logger("col:" + descriptor.shows)
  logger.debug("Opening column index files for projection " + descriptor.shows + " at " + baseDir)

  override def toString = "JDBMProjection(" + descriptor.columns + ")"

  private[this] final val treeMapName = "byIdentityMap"

  protected lazy val idIndexFile: DB = DBMaker.openFile(baseDir.getCanonicalPath + "/jdbm").make()
  
  protected lazy val treeMap: IndexTree = {
    val treeMap: IndexTree = idIndexFile.getTreeMap(treeMapName)
    if (treeMap == null) {
      logger.debug("Creating new projection store")
      idIndexFile.createTreeMap(treeMapName, IdentitiesComparator, IdentitiesSerializer, CValueSerializer)
    } else {
      treeMap
    }
  }

  def close() = {
    logger.info("Closing column index files")
    idIndexFile.commit()
    idIndexFile.close()
  }

  def insert(ids : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit] = IO {
    logger.trace("Inserting %s => %s".format(ids, v))
    treeMap.put(ids, v.toArray.asInstanceOf[Array[CValue]])

    if (shouldSync) {
      idIndexFile.commit()
    }
  }

  def allRecords(expiresAt: Long): IterableDataset[Seq[CValue]] = new IterableDataset(descriptor.identities, new Iterable[(Identities,Seq[CValue])] {
    def iterator = treeMap.entrySet.iterator.asScala.map { case kvEntry => (kvEntry.getKey, kvEntry.getValue) }
  })

  def getBlockAfter(id: Option[Identities]): Option[Slice] = {
    try {
      val constrainedMap = id.map(treeMap.tailMap(_)).getOrElse(treeMap)

      constrainedMap.lastKey() // Will throw an exception if the map is empty

      Some(new JDBMSlice(constrainedMap, descriptor, DEFAULT_SLICE_SIZE))
    } catch {
      case e: java.util.NoSuchElementException => None
    }
  }
}

