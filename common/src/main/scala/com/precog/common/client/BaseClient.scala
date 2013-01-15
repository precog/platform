package com.precog.common
package client

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.core.data._
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb

import scalaz._

object BaseClient {
  /**
   * A client's monad is, essentially, a future. However, there is a
   * chance that the server could be doing wonky things, so we create a side
   * channel, the left side of the EitherT, for reporting errors related
   * strictly to unexpected communication issues with the server.
   */
  type Response[+A] = EitherT[Future, String, A]

  case class ClientException(message: String) extends Exception(message)
}

trait BaseClient {
  import BaseClient._
  implicit def M: Monad[Future]
  
  def Response[A](a: Future[A]): Response[A] = EitherT.right(a)
  def BadResponse(msg: String): Response[Nothing] = EitherT.left(M.point(msg))

  /**
   * A natural transformation from Response to Future that maps the left side
   * to exceptions thrown inside the future.
   */
  implicit def ResponseAsFuture = new (Response ~> Future) {
    def apply[A](res: Response[A]): Future[A] = res.fold({ error => throw ClientException(error) }, identity)
  }

  implicit def FutureAsResponse = new (Future ~> Response) {
    def apply[A](fa: Future[A]): Response[A] = EitherT.eitherT(fa.map(\/.right).recoverWith {
      case ClientException(msg) => M.point(\/.left[String, A](msg))
    })
  }
  
  implicit def FutureStreamAsResponseStream = implicitly[Hoist[StreamT]].hoist(FutureAsResponse)
  implicit def ResponseStreamAsFutureStream(implicit M: Monad[Response]) = implicitly[Hoist[StreamT]].hoist(ResponseAsFuture)

  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A 
  protected def withJsonClient[A](f: HttpClient[ByteChunk] => A): A 
}

abstract class WebClient(protocol: String, host: String, port: Int, path: String)(implicit executor: ExecutionContext) extends BaseClient {
  final protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb
    f(client.protocol(protocol).host(host).port(port).path(path))
  }

  // This could be JValue, but too many problems arise w/ ambiguous implicits.
  final protected def withJsonClient[A](f: HttpClient[ByteChunk] => A): A = withRawClient { client =>
    f(client.contentType[ByteChunk](application/MimeTypes.json))
  }
}


// vim: set ts=4 sw=4 et:
