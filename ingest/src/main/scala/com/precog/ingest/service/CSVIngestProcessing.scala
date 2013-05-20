package com.precog.ingest
package service

import akka.dispatch.{ExecutionContext, Future, Promise}

import au.com.bytecode.opencsv.CSVReader

import blueeyes.core.data.ByteChunk
import blueeyes.core.http.HttpRequest
import blueeyes.json._

import com.precog.common.Path
import com.precog.common.ingest._
import com.precog.common.jobs.JobId
import com.precog.common.security.{APIKey, Authorities}
import com.precog.ingest.util.CsvType
import IngestProcessing._

import com.weiglewilczek.slf4s.Logging

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}

import scala.annotation.tailrec

import scalaz._

class CSVIngestProcessing(apiKey: APIKey, path: Path, authorities: Authorities, batchSize: Int, ingestStore: IngestStore)(implicit M: Monad[Future]) extends IngestProcessing {

  def forRequest(request: HttpRequest[_]): ValidationNel[String, IngestProcessor] = {
    val delimiter = request.parameters get 'delimiter
    val quote = request.parameters get 'quote
    val escape = request.parameters get 'escape

    Success(new IngestProcessor(delimiter, quote, escape))
  }

  final class IngestProcessor(delimiter: Option[String], quote: Option[String], escape: Option[String]) extends IngestProcessorLike with Logging {
    import scalaz.syntax.apply._
    import scalaz.Validation._

    def writeChunkStream(chan: WritableByteChannel, chunk: ByteChunk): Future[Long] = {
      chunk match {
        case Left(bytes) => writeChannel(chan, bytes :: StreamT.empty[Future, Array[Byte]], 0L)
        case Right(stream) => writeChannel(chan, stream, 0L)
      }
    }

    def writeToFile(byteStream: ByteChunk): Future[(File, Long)] = {
      val file = File.createTempFile("async-ingest-", null)
      val outChannel = new FileOutputStream(file).getChannel()
      for (written <- writeChunkStream(outChannel, byteStream)) yield (file, written)
    }

    final private def writeChannel(chan: WritableByteChannel, stream: StreamT[Future, Array[Byte]], written: Long): Future[Long] = {
      stream.uncons flatMap {
        case Some((bytes, tail)) =>
          val written0 = chan.write(ByteBuffer.wrap(bytes))
          writeChannel(chan, tail, written + written0)

        case None =>
          M.point { chan.close(); written }
      }
    }

    def readerBuilder: ValidationNel[String, java.io.Reader => CSVReader] = {
      def charOrError(s: Option[String], default: Char): ValidationNel[String, Char] = {
        s map {
          case s if s.length == 1 => success(s.charAt(0))
          case _ => failure("Expected a single character but found a string.")
        } getOrElse {
          success(default)
        } toValidationNel
      }

      val delimiterV = charOrError(delimiter, ',')
      val quoteV     = charOrError(quote, '"')
      val escapeV    = charOrError(escape, '\\')

      (delimiterV |@| quoteV |@| escapeV) { (delimiter, quote, escape) =>
        (reader: java.io.Reader) => new CSVReader(reader, delimiter, quote, escape)
      }
    }

    @tailrec final def readBatch(reader: CSVReader, batch: Vector[Array[String]]): (Boolean, Vector[Array[String]]) = {
      if (batch.size >= batchSize) {
        (false, batch)
      } else {
        val nextRow = reader.readNext()
        if (nextRow == null) (true, batch) else readBatch(reader, batch :+ nextRow)
      }
    }

    /**
      * Normalize headers by turning them into `JPath`s. Normally, a field will
      * be mapped to a `JPath` simply by wrapping it in a `JPathField`. However,
      * in the case of duplicate headers, we turn that field into an array. So,
      * the header a,a,a will create objects of the form `{a:[_, _, _]}`.
      */
    def normalizeHeaders(headers: Array[String]): Array[JPath] = {
      val positions = headers.zipWithIndex.foldLeft(Map.empty[String, List[Int]]) {
        case (hdrs, (h, i)) =>
          val pos = i :: hdrs.getOrElse(h, Nil)
          hdrs + (h -> pos)
      }

      positions.toList.flatMap {
        case (h, Nil) =>
          Nil
        case (h, pos :: Nil) =>
          (pos -> JPath(JPathField(h))) :: Nil
        case (h, ps) =>
          ps.reverse.zipWithIndex map { case (pos, i) =>
              (pos -> JPath(JPathField(h), JPathIndex(i)))
          }
      }.sortBy(_._1).map(_._2).toArray
    }

    def ingestSync(reader: CSVReader, jobId: Option[JobId], streamRef: StreamRef): Future[IngestResult] = {
      def readBatches(paths: Array[JPath], reader: CSVReader, total: Int, ingested: Int, errors: Vector[(Int, String)]): Future[IngestResult] = {
        // TODO: handle errors in readBatch
        M.point(readBatch(reader, Vector())) flatMap { case (done, batch) =>
          if (batch.isEmpty) {
            // the batch will only be empty if there's nothing left to read, but the batch size 
            // boundary was hit on the previous read and so it was not discovered that we didn't
            // need to continue until now. This could be cleaner via a more CPS'ed style, but meh.
            // This empty record is just stored to send the terminated streamRef.
            ingestStore.store(apiKey, path, authorities, Nil, jobId, streamRef.terminate) flatMap { _ =>
              M.point(BatchResult(total, ingested, errors))
            }
          } else {
            val types = CsvType.inferTypes(batch.iterator)
            val jvals = batch map { row =>
              (paths zip types zip row).foldLeft(JUndefined: JValue) { case (obj, ((path, tpe), s)) =>
                  JValue.unsafeInsert(obj, path, tpe(s))
              }
            }

            ingestStore.store(apiKey, path, authorities, jvals, jobId, if (done) streamRef.terminate else streamRef) flatMap { _ =>
              if (done) M.point(BatchResult(total + batch.length, ingested + batch.length, errors))
              else readBatches(paths, reader, total + batch.length, ingested + batch.length, errors)
            }
          }
        }
      }

      M.point(reader.readNext()) flatMap { header =>
        if (header == null) {
          M.point(NotIngested("No CSV data was found in the request content."))
        } else {
          readBatches(normalizeHeaders(header), reader, 0, 0, Vector())
        }
      }
    }

    def ingest(durability: Durability, errorHandling: ErrorHandling, storeMode: StoreMode, data: ByteChunk): Future[IngestResult] = {
      readerBuilder map { f =>
        for {
          (file, size) <- writeToFile(data)
          result <- ingestSync(f(new InputStreamReader(new FileInputStream(file), "UTF-8")), durability.jobId, storeMode.createStreamRef(false))
        } yield {
          file.delete()
          result
        }
      } valueOr { errors =>
        M.point(NotIngested(errors.list.mkString("; ")))
      }
    }
  }
}
