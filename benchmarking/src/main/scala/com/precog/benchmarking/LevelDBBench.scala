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

import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory

import java.io.File
import java.nio.ByteBuffer
import java.util.Arrays

object LevelDBBench extends Bench {
  val name = "LevelDBBench"
  val maxOpenFiles = 25

  val createOptions = (new Options)
    .createIfMissing(true)
    .maxOpenFiles(maxOpenFiles)
    .blockSize(1024 * 1024) // Based on rudimentary benchmarking. Gains in the high single digit percents
    
  def performWrites(baseDir: File, elementCount: Long) {
    val db = JniDBFactory.factory.open(new File(baseDir, "idIndex"), createOptions)

    var current = 0l
    val bytes = new Array[Byte](8)
    val buffer = ByteBuffer.wrap(bytes)

    while (current < elementCount) {
      buffer.clear()
      buffer.putLong(current)

      db.put(bytes, bytes)
      current += 1
    }
    
    // Close to force writes to be part of timing
    db.close()
  }

  def performReads(baseDir: File, elementCount: Long) {
    val dbRead = JniDBFactory.factory.open(new File(baseDir, "idIndex"), createOptions)

    // Read the full table
    val iter = dbRead.iterator()

    iter.seekToFirst()

    while (iter.hasNext()) {
      val n = iter.next()

      if (! Arrays.equals(n.getKey(), n.getValue()) ) {
        logger.error("Invalid result!")
      }
    }

    iter.close()
    dbRead.close()
  }
}
