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

  def close(): RawReader = {
    // start locking here?
    ps.close()
    new RawReader(id, log, rows, segments)
    // end locking here?
  }
}
