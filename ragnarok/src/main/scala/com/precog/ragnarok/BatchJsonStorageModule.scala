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
package com.precog
package ragnarok

import com.precog.yggdrasil._
import com.precog.common._

import java.util.zip.{ ZipFile, ZipEntry, ZipException }
import java.io.{ File, InputStreamReader, FileReader }

import blueeyes.json._
import blueeyes.json.JsonAST._

import akka.dispatch.Await

import scalaz._
import scalaz.effect._

import com.weiglewilczek.slf4s.Logging


/**
 * Provides a simple interface for ingesting bulk JSON data.
 */
trait BatchJsonStorageModule[M[+_]] extends StorageModule[M] with Logging {
  import scalaz.syntax.copointed._

  implicit def coM: Copointed[M]

  private val pid = System.currentTimeMillis.toInt & 0x7fffffff
  private val sid = new java.util.concurrent.atomic.AtomicInteger(0)

  private def openZipFile(f: File): Option[ZipFile] = try {
    Some(new ZipFile(f))
  } catch {
    case _: ZipException => None
  }

  /**
   * Reads a JArray from a JSON file or a set of JSON files zipped up together.
   */
  private def readRows(data: File): Iterator[JValue] = {
    // TODO Resource leak; need to close zippedData.
    openZipFile(data) map { zippedData =>
      new Iterator[ZipEntry] {
        val enum = zippedData.entries
        def next() = enum.nextElement()
        def hasNext = enum.hasMoreElements()
      } map { zipEntry =>
        new InputStreamReader(zippedData.getInputStream(zipEntry))
      } flatMap { reader =>
        val rows = JsonParser.parse(reader).children.toIterator
        reader.close()
        rows
      }
    } getOrElse {
      val reader = new FileReader(data)
      JsonParser.parse(reader).children.toIterator
    }
  }

  /**
   * Reads in the JSON file (or several zipped JSON files) into the specified
   * DB.
   */
  def ingest(db: String, data: File, tokenId: String = "root", batchSize: Int = 1000): IO[Unit] = IO {
    logger.debug("Ingesting %s to '//%s'." format (data, db))

    // Same as used by YggUtil's import command.
    val events = readRows(data) map { jval =>
      EventMessage(EventId(pid, sid.getAndIncrement),
        Event(Path(db), tokenId, jval, Map.empty))
    }

    events.grouped(batchSize).zipWithIndex foreach { case (batch, id) =>
      storage.storeBatch(batch).copoint
    }

    logger.debug("Ingested %s." format data)
  }
}

