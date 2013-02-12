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
