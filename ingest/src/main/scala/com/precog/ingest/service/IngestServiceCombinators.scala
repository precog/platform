package com.precog.ingest
package service

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.precog.common.Path
import com.precog.common.security._

trait IngestServiceCombinators extends TokenServiceCombinators {

  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  def left[A, B, C](h: HttpService[Either[A, B], C]): HttpService[A, C] = {
    new CustomHttpService[A, C] {
      val service = (req: HttpRequest[A]) =>
        h.service(req.copy(content = req.content map (Left(_))))

      val metadata = None
    }
  }

  def right[A, B, C](h: HttpService[Either[A, B], C]): HttpService[B, C] = {
    new CustomHttpService[B, C] {
      val service = (req: HttpRequest[B]) =>
        h.service(req.copy(content = req.content map (Right(_))))

      val metadata = None
    }
  }

  /**
   * If the request has no content, then it is treated as a JSON-P request and
   * the `content` paramter is given as the content of the JSON-P request as a
   * `Future[JValue]`. Otherwise, the content is streamed in as a `ByteChunk`.
   */
  def jsonpOrChunk(h: HttpService[Either[Future[JValue], ByteChunk], Future[HttpResponse[JValue]]]) = {
    new CustomHttpService[ByteChunk, Future[HttpResponse[ByteChunk]]] {
      val service = (request: HttpRequest[ByteChunk]) => {
        (if (request.content.isEmpty) {
          jsonp[ByteChunk](left(h))
        } else {
          produce(MimeTypes.application/MimeTypes.json)(right(h))
        }).service(request)
      }

      val metadata = None
    }
  }

  def dataPath[A, B](prefix: String)(next: HttpService[A, (Token, Path) => Future[B]]) = {
    path("""/%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n\.])*)/?)?)""".format(prefix)) { 
      new DelegatingService[A, Token => Future[B], A, (Token, Path) => Future[B]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (token: Token) => f(token, Path(path.getOrElse(""))) }
        }

        val metadata = None
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
