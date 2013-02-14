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
  // file doesn't exist -> create new file
  def empty(id: Long, f: File): RawHandler = {
    val ps = new PrintStream(new FileOutputStream(f, true), false, "UTF-8")
    RawLoader.writeHeader(ps, id)
    new RawHandler(id, f, Nil, ps)
  }

  // file does exist and is ok -> load data
  def load(id: Long, f: File): (RawHandler, Seq[Long], Boolean) = {
    val (rows, events, ok) = RawLoader.load(id, f)
    val ps = new PrintStream(new FileOutputStream(f, true), false, "UTF-8")
    (new RawHandler(id, f, rows, ps), events, ok)
  }
}

// TODO: extend derek's reader/writer traits
class RawHandler private[niflheim] (val id: Long, val log: File, rs: Seq[JValue], ps: PrintStream) {
  private val rows = mutable.ArrayBuffer.empty[JValue] ++ rs // TODO: weakref?
  private var segments = Segments.empty(id) // TODO: weakref?
  private var count = rows.length

  def length: Int = count

  def snapshot(): Segments = if (rows.isEmpty) {
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

  /**
["rawlog", blockid, version]
...

##rawlog blockid version
##start <932,123>
json1
json2
json3
##end <932,123>
##start <932,9991>
...
##end <932,9991>
##start <123,9923>
...EOF
   */

  def write(eventid: Long, values: Seq[JValue]) {
    // start locking here?
    if (!values.isEmpty) {
      count += values.length
      RawLoader.writeEvents(ps, eventid, values)
      rows ++= values
    }
    // end locking here?
  }

  def close(): Unit = ps.close()
}
