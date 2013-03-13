package com.precog.ragnarok

import com.precog.yggdrasil._
import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.util.PrecogUnit

import java.util.zip.{ ZipFile, ZipEntry, ZipException }
import java.io.{ File, InputStreamReader, FileReader, BufferedReader }

import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.table._

import blueeyes.json._

import scalaz._
import scalaz.effect._
import scalaz.syntax.copointed._

import akka.dispatch._
import akka.actor.{ IO => _, _ }

import com.weiglewilczek.slf4s.Logging

/**
 * Provides a simple interface for ingesting bulk JSON data.
 */
trait NIHDBIngestSupport extends NIHDBColumnarTableModule with Logging {
  implicit def M: Monad[Future] with Copointed[Future]
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
  def ingest(db: String, data: File, apiKey: String = "root", accountId: String = "root"): IO[PrecogUnit] = IO {
    logger.debug("Ingesting %s to '//%s'." format (data, db))

    implicit val to = storageTimeout

    val path = Path(db)
    val eventId = EventId(pid, sid.getAndIncrement)
    val records = (eventId.uid, readRows(data) map (IngestRecord(eventId, _)))

    val projection = (projectionsActor ? ProjectionInsert(path, Seq(records), Authorities(accountId))).flatMap { _ =>
      logger.debug("Insert complete on //%s, waiting for cook".format(db))

      (projectionsActor ? FindProjection(path)).mapTo[NIHDBActorProjection]
    }.copoint

    while (projection.status.copoint.pending > 0) {
      Thread.sleep(100)
    }
    projection.close(actorSystem).copoint

    logger.debug("Ingested %s." format data)

    PrecogUnit
  }
}
