package com.precog.ingest
package service

import akka.dispatch.{ExecutionContext, Future}

import blueeyes.core.data.ByteChunk
import blueeyes.core.http.MimeType
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.HttpRequest
import blueeyes.core.http.MimeTypes._
import blueeyes.json.{AsyncParser, AsyncParse }
import AsyncParser.{More, Done}

import com.precog.common.Path
import com.precog.common.ingest._
import com.precog.common.jobs.JobId
import com.precog.common.security.{APIKey, Authorities}

import java.nio.ByteBuffer

import scala.annotation.tailrec

import scalaz._
import scalaz.syntax.std.boolean._

object IngestProcessing {
  sealed trait ErrorHandling
  case object StopOnFirstError extends ErrorHandling
  case object AllOrNothing extends ErrorHandling
  case object IngestAllPossible extends ErrorHandling

  sealed trait Durability { def jobId: Option[JobId] }
  case object LocalDurability extends Durability { val jobId = None }
  case class GlobalDurability(jid: JobId) extends Durability { val jobId = Some(jid) }

  sealed trait IngestResult
  case class BatchResult(total: Int, ingested: Int, errors: Vector[(Int, String)]) extends IngestResult
  case class StreamingResult(ingested: Int, error: Option[String]) extends IngestResult
  case class NotIngested(reason: String) extends IngestResult

  val JSON = application/json
  val JSON_STREAM = MimeType("application", "x-json-stream")
  val CSV = text/csv

  /** Chain of responsibility element used to determine a IngestProcessing strategy */
  @tailrec final def select(from: List[IngestProcessingSelector], partialData: Array[Byte], request: HttpRequest[_]): Option[IngestProcessing] = {
    from match {
      case hd :: tl =>
        hd.select(partialData, request) match { // not using map so as to get tailrec
          case None => select(tl, partialData, request)
          case some => some
        }

      case Nil => None
    }
  }
}

trait IngestProcessing {
  import IngestProcessing._

  type IngestProcessor <: IngestProcessorLike 

  /**
   * Build an ingest processor based only upon the request metadata. The type of HttpRequest is existential here
   * specifically to prohibit implementations from peeking at the data.
   */
  def forRequest(request: HttpRequest[_]): ValidationNel[String, IngestProcessor]

  trait IngestProcessorLike {
    def ingest(durability: Durability, errorHandling: ErrorHandling, storeMode: StoreMode, data: ByteChunk): Future[IngestResult]
  }
}

trait IngestProcessingSelector {
  def select(partialData: Array[Byte], request: HttpRequest[_]): Option[IngestProcessing]
}

class DefaultIngestProcessingSelectors(maxFields: Int, batchSize: Int, ingestStore: IngestStore)(implicit M: Monad[Future], executor: ExecutionContext){
  import IngestProcessing._

  class MimeIngestProcessingSelector(apiKey: APIKey, path: Path, authorities: Authorities) extends IngestProcessingSelector {
    def select(partialData: Array[Byte], request: HttpRequest[_]): Option[IngestProcessing] = {
      request.headers.header[`Content-Type`].toSeq.flatMap(_.mimeTypes) collectFirst {
        case JSON => new JSONIngestProcessing(apiKey, path, authorities, JSONValueStyle, maxFields, ingestStore)
        case JSON_STREAM => new JSONIngestProcessing(apiKey, path, authorities, JSONStreamStyle, maxFields, ingestStore)
        case CSV => new CSVIngestProcessing(apiKey, path, authorities, batchSize, ingestStore)
      }
    }
  }

  class JSONIngestProcessingSelector(apiKey: APIKey, path: Path, authorities: Authorities) extends IngestProcessingSelector {
    def select(partialData: Array[Byte], request: HttpRequest[_]): Option[IngestProcessing] = {
      val (AsyncParse(errors, values), parser) = AsyncParser.stream().apply(More(ByteBuffer.wrap(partialData)))
      if (errors.isEmpty && !values.isEmpty) {
        request.headers.header[`Content-Type`].toSeq.flatMap(_.mimeTypes) collectFirst {
          case JSON_STREAM => new JSONIngestProcessing(apiKey, path, authorities, JSONStreamStyle, maxFields, ingestStore)
        } orElse {
          Some(new JSONIngestProcessing(apiKey, path, authorities, JSONValueStyle, maxFields, ingestStore))
        } 
      } else {
        None
      }
    }
  }

  def selectors(apiKey: APIKey, path: Path, authorities: Authorities): List[IngestProcessingSelector] = List(
    new MimeIngestProcessingSelector(apiKey, path, authorities),
    new JSONIngestProcessingSelector(apiKey, path, authorities)
  )
}
