package com.precog.shard
package service

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.JsonAST.JValue
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.precog.common.Path
import com.precog.common.security._
import com.precog.ingest.service._

import scalaz.Validation
import scalaz.Validation._

trait ShardServiceCombinators extends IngestServiceCombinators {

  type Query = String

  import BijectionsByteArray._
  import BijectionsChunkJson._
  import BijectionsChunkString._
  import BijectionsChunkFutureJson._

  // def query[A, B](next: HttpService[A, (Token, Path, Query) => Validation[NotServed, Future[B]]]) = {
  //   new DelegatingService[A, (Token, Path) => Future[B], A, (Token, Path, Query) => Future[B]] {
  //     val delegate = next
  //     val metadata = None
  //     val service = (request: HttpRequest[A]) => {
  //       val query: Option[String] = request.parameters.get('q).filter(_ != null)
  //       // val limit: Option[String] = request.parameters.get('limit).filter(_ != null)
  //       // val offset: Option[String] = request.parameters.get('offset).filter(_ != null)

  //       query map { q =>
  //         next.service(request) flatMap { f =>
  //           (token: Token, path: Path) => f(token, path, q)
  //         }
  //       } getOrElse {
  //         failure(inapplicable)
  //       }
  //     }
  //   }
  // }

  def query[A, B](next: HttpService[A, (Token, Path, Query) => Future[B]]) = {
    new DelegatingService[A, (Token, Path) => Future[B], A, (Token, Path, Query) => Future[B]] {
      val delegate = next
      val metadata = None
      val service = (request: HttpRequest[A]) => {
        val query: Option[String] = request.parameters.get('q).filter(_ != null)

        query map { q =>
          next.service(request) map { f => (token: Token, path: Path) => f(token, path, q) }
        } getOrElse {
          failure(inapplicable)
        }
      }
    }
  }

  def jsonpcb[A](delegate: HttpService[Future[JValue], Future[HttpResponse[A]]])(implicit bi: Bijection[A, ByteChunk]) =
    jsonpc[Array[Byte], Array[Byte]](delegate map (_ map { response =>
      response.copy(content = response.content map (bi(_)))
    }))
}
