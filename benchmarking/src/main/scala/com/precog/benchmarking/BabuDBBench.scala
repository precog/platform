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

import org.xtreemfs.babudb._
import org.xtreemfs.babudb.api._
import org.xtreemfs.babudb.config._

import java.io.File
import java.nio.ByteBuffer
import java.util.Arrays

import scala.collection.JavaConverters._

object BabuDBBench extends Bench {
  val name = "BabuDBBench"

  def performWrites(baseDir: File, elementCount: Long) {
    var current = 0l
    val bytes = new Array[Byte](8)
    val buffer = ByteBuffer.wrap(bytes)

    val dbSys = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath(baseDir.getCanonicalPath()).setMultiThreaded(4).build())
    val db = dbSys.getDatabaseManager().createDatabase("indexDB", 1)

    while (current < elementCount) {
      buffer.clear()
      buffer.putLong(current)

      db.singleInsert(0, bytes, bytes, null)
      current += 1
    }

    db.shutdown()
    dbSys.shutdown()
  }

  def performReads(baseDir: File, elementCount: Long) {
    val bytes = new Array[Byte](8)
    val buffer = ByteBuffer.wrap(bytes)

    val dbSysRead = BabuDBFactory.createBabuDB(new ConfigBuilder().setDataPath(baseDir.getCanonicalPath()).setMultiThreaded(4).build())
    val dbRead = dbSysRead.getDatabaseManager().getDatabase("indexDB")

    val firstIdx = { buffer.clear(); buffer.putLong(0l); bytes }
    val endIdx   = { buffer.clear(); buffer.putLong(Long.MaxValue); bytes }
    val result = dbRead.rangeLookup(0, firstIdx, endIdx, null)

    val iter = result.get()
    iter.asScala.forall { kvPair => Arrays.equals(kvPair.getKey(), kvPair.getValue()) }

    iter.free()

    dbRead.shutdown()
    dbSysRead.shutdown()
  }    
}
