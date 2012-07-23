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
package com.precog.benchmarking

import org.apache.jdbm._

import com.weiglewilczek.slf4s.Logging

import java.io.File
import java.util.SortedMap

import scala.collection.JavaConverters._

object JDBM3Bench extends Bench with Logging {
  val name = "JDBM3Bench"

  val mapName = "indexMap"

  def performWrites(baseDir: File, elementCount: Long) {
    var current = 0l
    val dbSys = DBMaker.openFile(baseDir.getCanonicalPath + "/indexFile").make()
    
    val db: SortedMap[java.lang.Long,Long] = dbSys.createTreeMap(mapName)

    while (current < elementCount) {
      db.put(current, current)
      if (current % 200000 == 0) {
        dbSys.commit() // Prevent heap overrun
      }
      current += 1
    }

    dbSys.commit()
    dbSys.close()
  }
    
  def performReads(baseDir: File, elementCount: Long) {
    val dbSysRead = DBMaker.openFile(baseDir.getCanonicalPath + "/indexFile").make()
    val dbRead: SortedMap[java.lang.Long,Long] = dbSysRead.getTreeMap(mapName)

    val (allEqual,count) = dbRead.entrySet().iterator().asScala.foldLeft((true,0l)) { case ((allEqual,count),kvPair) => (allEqual && kvPair.getKey() == kvPair.getValue(),count + 1) }

    if (!allEqual) {
      logger.error("Mismatch on retrievals!")
    }

    if (count != elementCount) {
      logger.error("Incorrect count on retrievals")
    }

    logger.info("Results = " + (allEqual,count))

    dbSysRead.close()
  }
}
