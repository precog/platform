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
