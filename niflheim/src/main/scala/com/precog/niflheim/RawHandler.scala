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
    val os = new BufferedOutputStream(new FileOutputStream(f, true))
    RawLoader.writeHeader(os, id)
    new RawHandler(id, f, Nil, os)
  }

  // file does exist and is ok -> load data
  def load(id: Long, f: File): (RawHandler, Seq[Long], Boolean) = {
    val (rows, events, ok) = RawLoader.load(id, f)
    val os = new BufferedOutputStream(new FileOutputStream(f, true))
    (new RawHandler(id, f, rows, os), events, ok)
  }
}

// TODO: extend derek's reader/writer traits
class RawHandler private[niflheim] (val id: Long, val log: File, rs: Seq[JValue], os: OutputStream) extends StorageReader {
  @volatile
  private var rows = mutable.ArrayBuffer.empty[JValue] ++ rs // TODO: weakref?
  @volatile
  private var segments = Segments.empty(id) // TODO: weakref?
  private var count = rows.length

  def structure = snapshot(None).map { seg => (seg.cpath, seg.ctype) }

  def length: Int = count

  def snapshot(pathConstraint: Option[Set[CPath]]): Seq[Segment] = {
    if (!rows.isEmpty) {
      segments.synchronized {
        if (!rows.isEmpty) {
          segments.extendWithRows(rows)
          rows.clear()
        }
        segments
      }
    }

    pathConstraint.map { cpaths =>
      segments.a.filter { seg => cpaths(seg.cpath) }
    }.getOrElse(segments.a)
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
      RawLoader.writeEvents(os, eventid, values)
      rows ++= values
    }
    // end locking here?
  }

  def close(): Unit = {
    os.close()
  }
}
