package com.precog.niflheim

import blueeyes.json._
import scala.collection.mutable
import org.joda.time.DateTime
import java.io._

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

private[niflheim] object RawLoader {
  def load(id: Long, f: File): Seq[JValue] = {
    val rows = JParser.parseManyFromFile(f).valueOr(throw _)
    rows.headOption match {
      case None =>
        sys.error("attempt to load empty file: %s" format f)
        
      case Some(JArray(JString("rawlog") :: (jid:JNum) :: (jversion:JNum) :: Nil)) =>
        val blockid = jid.toLong
        val version = jversion.toLong
        if (blockid != id)
          sys.error("bad rawlog blockid; found %s, expected %s" format (blockid, id))

        version match {
          case 1 => rows.tail
          case n => sys.error("unknown rawlog version: %d" format n)
        }

      case Some(o) =>
        sys.error("invalid rawlog header: %s" format o)
    }
  }
}
