package com.precog.niflheim

import blueeyes.json._
import scala.collection.mutable
import org.joda.time.DateTime
import java.io._

import com.precog.common._
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

  def loadReadonly(id: Long, f: File): (RawReader, Seq[Long], Boolean) = {
    val (rows, events, ok) = RawLoader.load(id, f)
    (new RawReader(id, f, rows), events, ok)
  }
}

class RawReader private[niflheim] (val id: Long, val log: File, rs: Seq[JValue]) extends StorageReader {
  // TODO: weakrefs?
  @volatile protected[this] var rows = mutable.ArrayBuffer.empty[JValue] ++ rs
  @volatile protected[this] var segments = Segments.empty(id)
  protected[this] var count = rows.length

  protected[this] val rowLock = new Object

  def isStable: Boolean = true

  def structure: Iterable[ColumnRef] =
    snapshot(None).segments.map { seg => ColumnRef(seg.cpath, seg.ctype) }

  def length: Int = count

  def handleNonempty = {
    if (!rows.isEmpty) {
      rowLock.synchronized {
        if (!rows.isEmpty) {
          segments.extendWithRows(rows)
          rows.clear()
        }
        segments
      }
    }
  }

  def snapshot(pathConstraint: Option[Set[CPath]]): Block = {
    handleNonempty

    val segs = pathConstraint.map { cpaths =>
      segments.a.filter { seg => cpaths(seg.cpath) }
    }.getOrElse(segments.a.clone)

    Block(id, segs, isStable)
  }

  def snapshotRef(refConstraints: Option[Set[ColumnRef]]): Block = {
    handleNonempty

    val segs = refConstraints.map { refs =>
      segments.a.filter { seg => refs(ColumnRef(seg.cpath, seg.ctype)) }
    }.getOrElse(segments.a.clone)

    Block(id, segs, isStable)
  }
}

class RawHandler private[niflheim] (id: Long, log: File, rs: Seq[JValue], private var os: OutputStream) extends RawReader(id, log, rs) {
  def write(eventid: Long, values: Seq[JValue]) {
    if (!values.isEmpty) {
      rowLock.synchronized {
        count += values.length
        RawLoader.writeEvents(os, eventid, values)
        rows ++= values
      }
    }
  }

  override def isStable: Boolean = os == null

  def close(): Unit = if (os != null) {
    os.close()
    os = null
  }
}
