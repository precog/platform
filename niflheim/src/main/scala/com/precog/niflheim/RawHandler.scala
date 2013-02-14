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
    if (f.exists)
      sys.error("rawlog %s already exists!" format f)
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
