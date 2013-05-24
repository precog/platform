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
package com.precog.ragnarok

import blueeyes.json._
import blueeyes.util.Clock

import com.precog.yggdrasil._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.niflheim.NIHDB
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.vfs._
import com.precog.yggdrasil.actor.IngestData

import java.util.zip.{ ZipFile, ZipEntry, ZipException }
import java.io.{ File, InputStreamReader, FileReader, BufferedReader }

import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.table._

import scalaz._
import scalaz.effect._
import scalaz.syntax.comonad._

import akka.dispatch._
import akka.actor.{ IO => _, _ }
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

/**
 * Provides a simple interface for ingesting bulk JSON data.
 */
trait NIHDBIngestSupport extends NIHDBColumnarTableModule with ActorVFSModule with Logging {
  implicit def M: Monad[Future] with Comonad[Future]
  def actorSystem: ActorSystem

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
  private def readRows(data: File): Seq[JValue] = {
    // TODO Resource leak; need to close zippedData.
    openZipFile(data).map { zippedData =>
      new Iterator[ZipEntry] {
        val enum = zippedData.entries
        def next() = enum.nextElement()
        def hasNext = enum.hasMoreElements()
      }.map { zipEntry =>
        new InputStreamReader(zippedData.getInputStream(zipEntry))
      }.flatMap { reader =>
        val sb = new StringBuilder
        val buf = new BufferedReader(reader)
        var line = buf.readLine
        while (line != null) {
          sb.append(line)
          line = buf.readLine
        }
        val str = sb.toString
        val rows = JParser.parseManyFromString(str).valueOr(throw _).toIterator
        reader.close()
        rows
      }.toList
    } getOrElse {
      JParser.parseManyFromFile(data).valueOr(throw _)
    }
  }

  /**
   * Reads in the JSON file (or several zipped JSON files) into the specified
   * DB.
   */
  def ingest(db: String, data: File, apiKey: String = "root", accountId: String = "root", clock: Clock = Clock.System): IO[PrecogUnit] = IO {
    logger.debug("Ingesting %s to '//%s'." format (data, db))

    implicit val to = Timeout(300 * 1000)

    val path = Path(db)
    val eventId = EventId(pid, sid.getAndIncrement)
    val records = readRows(data) map (IngestRecord(eventId, _))

    val projection = {
      for {
        _ <- M point { 
          vfs.writeAll(Seq((0, IngestMessage(apiKey, path, Authorities(accountId), records, None, clock.instant, StreamRef.Append)))).unsafePerformIO 
        }
        _ = logger.debug("Insert complete on //%s, waiting for cook".format(db))
        projection <- vfs.readProjection(apiKey, path, Version.Current).run
      } yield {
        (projection valueOr { err => sys.error("An error was encountered attempting to read projection at path %s: %s".format(path, err.toString)) }).asInstanceOf[NIHDBResource]
      }
    }.copoint

    while (projection.db.status.copoint.pending > 0) {
      Thread.sleep(100)
    }

    projection.db.close(actorSystem).copoint

    logger.debug("Ingested %s." format data)

    PrecogUnit
  }
}
