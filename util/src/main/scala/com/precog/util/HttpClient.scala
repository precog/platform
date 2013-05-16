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
package com.precog.util

import java.net.URL

import scalaz._

/**
 * Very stupid-simple HTTP client.  If we need something more powerful, speak
 * to the management.
 */
trait HttpClientModule[M[+_]] {
  implicit def M: Monad[M]
  
  type HttpClient <: HttpClientLike
  
  trait HttpClientLike {
    def execute(request: Request[String]): EitherT[M, HttpClientError, Response[String]]
  }

  def HttpClient(baseUrl: String): HttpClient

  sealed trait HttpMethod
  object HttpMethod {
    object GET extends HttpMethod
    object POST extends HttpMethod
  }

  sealed trait HttpClientError
  object HttpClientError {
    case class ConnectionError(url: Option[String], cause: Throwable) extends HttpClientError
    case class NotOk(code: Int, message: String) extends HttpClientError
    case object EmptyBody extends HttpClientError
  }

  case class Request[+A](
      method: HttpMethod = HttpMethod.GET,
      path: String = "",
      params: List[(String, String)] = Nil,
      body: Option[Request.Body[A]] = None) {

    def /(part: String): Request[A] = copy(path = path + "/" + part)
    def ?(p: (String, String)): Request[A] = copy(params = p :: params)
    def &(p: (String, String)): Request[A] = copy(params = p :: params)

    def withBody[B](contentType: String, body: B): Request[B] =
      copy(body = Some(Request.Body(contentType, body)))

    def map[B](f: A => B): Request[B] =
      copy(body = body map { case Request.Body(ct, data) => Request.Body(ct, f(data)) })
  }

  object Request {
    case class Body[+A](contentType: String, data: A)
  }

  case class Response[+A](code: Int, message: String, body: Option[A]) {
    import HttpClientError._

    def map[B](f: A => B): Response[B] = Response(code, message, body map f)

    def ok: HttpClientError \/ A = body map { data =>
      if (code / 200 != 1) -\/(NotOk(code, message)) else \/-(data)
    } getOrElse -\/(EmptyBody)
  }
}
