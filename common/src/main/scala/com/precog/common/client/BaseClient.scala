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

  /**
   * A natural transformation from Response to Future that maps the left side
   * to exceptions thrown inside the future.
   */
  implicit def ResponseAsFuture(implicit M: Monad[Future]) = new (Response ~> Future) {
    def apply[A](res: Response[A]): Future[A] = res.fold({ error => throw ClientException(error) }, identity)
  }

  implicit def FutureAsResponse(implicit M: Monad[Future]) = new (Future ~> Response) {
    def apply[A](fa: Future[A]): Response[A] = EitherT.eitherT(fa.map(\/.right).recoverWith {
      case ClientException(msg) => M.point(\/.left[String, A](msg))
    })
  }
  
  implicit def FutureStreamAsResponseStream(implicit M: Monad[Future]) = implicitly[Hoist[StreamT]].hoist(FutureAsResponse)
  implicit def ResponseStreamAsFutureStream(implicit MF: Monad[Future], MR: Monad[Response]) = implicitly[Hoist[StreamT]].hoist(ResponseAsFuture)
}

trait BaseClient {
  import BaseClient._
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
