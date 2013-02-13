package com.precog.niflheim

import blueeyes.json._
import scala.collection.mutable
import org.joda.time.DateTime
import java.io._

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

object RawReader {
  def load(id: Long, f: File): RawReader =
    new RawReader(id, f, RawLoader.load(id, f), Segments.empty(id))
}

class RawReader private[niflheim] (val id: Long, val log: File, r: Seq[JValue], s: Segments) {
  private var rows: Seq[JValue] = r
  private var segments: Segments = s

  val length = r.length + s.length

  def snapshot(): Segments = if (rows.isEmpty) {
    segments 
  } else {
    val segs = segments.copy
    segs.extendWithRows(rows)
    // start locking here?
    rows = Nil
    segments = segs
    // end locking here?
    segs
  }
}
