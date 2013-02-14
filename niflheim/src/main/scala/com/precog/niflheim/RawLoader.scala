package com.precog.niflheim

import scala.{specialized => spec}

import blueeyes.json._
import scala.collection.mutable
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import java.io._

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

private[niflheim] object RawLoader {
  private val fmt = DateTimeFormat.forPattern("yyyyMMddHHmmssSSS")

  private val utf8 = java.nio.charset.Charset.forName("UTF-8")

  /**
   * Write the rawlog header to 'ps'. Currently this is:
   * 
   *   "##rawlog <id> 1\n"
   */
  def writeHeader(ps: PrintStream, id: Long): Unit =
    ps.println("##rawlog " + id.toString + " 1")

  /**
   * Write the given event to 'ps'. Each event consists of an
   * 'eventid' and a sequence of Jvalue instances.
   */
  def writeEvents(ps: PrintStream, eventid: Long, values: Seq[JValue]) {
    val e = eventid.toString
    ps.println("##start " + e)
    values.foreach(j => ps.println(j.renderCompact))
    ps.println("##end " + e)
  }

  /**
   * Load the rawlog (using the version 1 format).
   *
   * This method assumes the header line has already been parsed, and
   * expects to see zero-or-more of the following groups:
   */
  def load1(id: Long, f: File, reader: BufferedReader): (Seq[JValue], Seq[Long], Boolean) = {
    val rows = mutable.ArrayBuffer.empty[JValue]
    val events = mutable.ArrayBuffer.empty[(Long, Int)]
    var line = reader.readLine()
    var ok = true
    while (ok && line != null) {
      if (line.startsWith("##start ")) {
        try {
          val eventid = line.substring(8).toLong
          val count = loadEvents1(reader, eventid, rows)
          if (count < 0) {
            ok = false
          } else {
            events.append((eventid, count))
            line = reader.readLine()
          }
        } catch {
          case _: Exception =>
            ok = false
        }
      }
    }
    if (!ok) recover1(id, f, rows, events)
    (rows, events.map(_._1), ok)
  }

  /**
   * Generate a "corrupted" rawlog file name.
   *
   * From "/foo/bar" we'l return "/foo/bar-corrupted-20130213155306768"
   */
  def getCorruptFile(f: File): File =
    new File(f.getPath + "-corrupted-" + fmt.print(new DateTime))

  /**
   * Recovery 
   */
  def recover1(id: Long, f: File, rows: mutable.ArrayBuffer[JValue], events: mutable.ArrayBuffer[(Long, Int)]) {

    // open a tempfile to write a "corrected" rawlog to, and write the header
    val tmp = File.createTempFile("nilfheim", "recovery")
    val ps = new PrintStream(new FileOutputStream(tmp, true), false, "UTF-8")
    writeHeader(ps, id)

    // for each event, write its rows to the rawlog
    var row = 0
    val values = mutable.ArrayBuffer.empty[JValue]
    events.foreach { case (eventid, count) =>
      var i = 0
      while (i < count) {
        values.append(rows(row))
        row += 1
        i += 1
      }
      writeEvents(ps, eventid, values)
      values.clear()
    }

    // rename the rawlog file to indicate corruption
    f.renameTo(getCorruptFile(f))

    // rename the tempfile to the rawlog file
    tmp.renameTo(f)
  }

  def isValidEnd1(line: String, eventid: Long): Boolean = try {
    line.substring(6).toLong == eventid
  } catch {
    case _: Exception => false
  }

  def loadEvents1(reader: BufferedReader, eventid: Long, rows: mutable.ArrayBuffer[JValue]): Int = {
    val sofar = mutable.ArrayBuffer.empty[JValue]

    var line = reader.readLine()
    var going = true
    var ok = false
    var count = 0

    while (going && line != null) {
      if (line.startsWith("##end ")) {
        going = false
        ok = isValidEnd1(line, eventid)
      } else {
        try {
          sofar.append(JParser.parseUnsafe(line))
          count += 1
          line = reader.readLine()
        } catch {
          case _: Exception =>
            going = false
        }
      }
    }
    if (ok) {
      rows ++= sofar
      count
    } else {
      -1
    }
  }

  def load(id: Long, f: File): (Seq[JValue], Seq[Long], Boolean) = {
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), utf8))
    try {
      val header = reader.readLine()
      if (header == null)
        sys.error("missing header")
      else if (header == ("##rawlog " + id.toString + " 1"))
        load1(id, f, reader)
      else
        sys.error("unsupported header: %s" format header)
    } finally {
      reader.close()
    }
  }
}
