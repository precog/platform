package com.precog.ingest
package service

import akka.dispatch.{ExecutionContext, Future}

import blueeyes.core.data.ByteChunk
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.HttpRequest
import blueeyes.core.http.MimeTypes._
import blueeyes.json.{AsyncParser, AsyncParse, JParser}

import com.precog.common.Path
import com.precog.common.jobs.JobId
import com.precog.common.security.{APIKey, Authorities}

import java.nio.ByteBuffer

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.std.boolean._

trait IngestProcessor {
  def processBatch(data: ByteChunk, parseDirectives: Set[ParseDirective], jobId: JobId, sync: Boolean): Future[BatchIngestResult]
  def processStream(data: ByteChunk, parseDirectives: Set[ParseDirective]): Future[StreamingIngestResult]
}

class IngestProcessorSelection(maxFields: Int, batchSize: Int, ingestStore: IngestStore)(implicit M: Monad[Future], executor: ExecutionContext){
  /** Chain of responsibility used to determine a IngestProcessor strategy */
  trait IngestProcessorSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[IngestProcessor]
  }

  class MimeIngestProcessorSelector(apiKey: APIKey, path: Path, authorities: Authorities) extends IngestProcessorSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[IngestProcessor] = {
      val JSON = application/json
      val CSV = text/csv

      parseDirectives collectFirst {
        case MimeDirective(JSON) => new JSONIngestProcessor(apiKey, path, authorities, maxFields, ingestStore)
        case MimeDirective(CSV) => new CSVIngestProcessor(apiKey, path, authorities, batchSize, ingestStore)
      }
    }
  }

  class JsonIngestProcessorSelector(apiKey: APIKey, path: Path, authorities: Authorities) extends IngestProcessorSelector {
    def select(partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[IngestProcessor] = {
      val (AsyncParse(errors, values), parser) = JParser.parseAsync(ByteBuffer.wrap(partialData))
      (errors.isEmpty && !values.isEmpty) option { new JSONIngestProcessor(apiKey, path, authorities, maxFields, ingestStore) }
    }
  }

  def ingestSelectors(apiKey: APIKey, path: Path, authorities: Authorities): List[IngestProcessorSelector] = List(
    new MimeIngestProcessorSelector(apiKey, path, authorities),
    new JsonIngestProcessorSelector(apiKey, path, authorities)
  )

  def getParseDirectives(request: HttpRequest[_]): Set[ParseDirective] = {
    val mimeDirective =
      for {
        header <- request.headers.header[`Content-Type`]
        mimeType <- header.mimeTypes.headOption
      } yield MimeDirective(mimeType)

    val delimiter = request.parameters get 'delimiter map { CSVDelimiter(_) }
    val quote = request.parameters get 'quote map { CSVQuote(_) }
    val escape = request.parameters get 'escape map { CSVEscape(_) }

    mimeDirective.toSet ++ delimiter ++ quote ++ escape
  }

  @tailrec final def selectIngestProcessor(from: List[IngestProcessorSelector], partialData: Array[Byte], parseDirectives: Set[ParseDirective]): Option[IngestProcessor] = {
    from match {
      case hd :: tl =>
        hd.select(partialData, parseDirectives) match { // not using map so as to get tailrec
          case None => selectIngestProcessor(tl, partialData, parseDirectives)
          case some => some
        }

      case Nil => None
    }
  }
}
