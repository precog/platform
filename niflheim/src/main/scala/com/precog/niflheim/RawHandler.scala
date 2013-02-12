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
package com.precog.niflheim

import blueeyes.json._
import scala.collection.mutable
import org.joda.time.DateTime
import java.io._

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

object RawHandler {
  def empty(id: Long, f: File): RawHandler = {
    val ps = new PrintStream(new FileOutputStream(f, true), false, "UTF-8")
    ps.print("[\"rawlog\", " + id.toString + ", 1]\n")
    new RawHandler(id, f, Nil, ps)
  }

  def load(id: Long, f: File): RawHandler = {
    val rows = RawLoader.load(id, f)
    val ps = new PrintStream(new FileOutputStream(f, true), false, "UTF-8")
    new RawHandler(id, f, rows, ps)
  }
}

class RawHandler private[niflheim] (val id: Long, val log: File, rs: Seq[JValue], ps: PrintStream) {
  private val rows = mutable.ArrayBuffer.empty[JValue] ++ rs
  private var segments = Segments.empty(id)
  private var count = rows.length

  def length: Int = count

  def snapshot: Segments = if (rows.isEmpty) {
    segments
  } else {
    val segs = segments.copy
    segs.extendWithRows(rows)
    // start locking here?
    rows.clear()
    segments = segs
    // end locking here?
    segs
  }

  def write(values: Seq[JValue]) {
    // start locking here?
    count += values.length
    values.foreach { j =>
      ps.print(j.renderCompact)
      ps.print('\n')
    }
    rows ++= values
    // end locking here?
  }

  def close: RawReader = {
    // start locking here?
    ps.close()
    new RawReader(id, log, rows, segments)
    // end locking here?
  }
}
