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

trait BaseClient {
  implicit def M: Monad[Future]
  
  def Response[A](a: Future[A]): Response[A] = EitherT.right(a)
  def BadResponse(msg: String): Response[Nothing] = EitherT.left(M.point(msg))

  protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A 
  
  // This could be JValue, but too many problems arise w/ ambiguous implicits.
  final protected def withJsonClient[A](f: HttpClient[ByteChunk] => A): A = withRawClient { client =>
    f(client.contentType[ByteChunk](application/MimeTypes.json))
  }
}

abstract class WebClient(protocol: String, host: String, port: Int, path: String)(implicit executor: ExecutionContext) extends BaseClient {
  val client = new HttpClientXLightWeb
  final protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = {
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
}


// vim: set ts=4 sw=4 et:
