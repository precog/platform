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

import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import scalaz._

trait EchoHttpClientModule[M[+_]] extends HttpClientModule[M] {
  private def wrapper(request: Request[String]): EitherT[M, HttpClientError, Response[String]] = {
    val response = Response(200, "OK", request.body map (_.data) map { data =>
      val json = JParser.parseUnsafe(data)
      val JString(field) = json \ "field"
      val JArray(elems) = json \ "data"
      val out = jobject("data" -> JArray(elems map { e => jobject(jfield(field, e)) }))
      out.renderCompact
    })
    EitherT(M point \/-(response))
  }

  private def echo(request: Request[String]): EitherT[M, HttpClientError, Response[String]] = {
    val response = Response(200, "OK", request.body map (_.data))
    EitherT(M point \/-(response))
  }

  private def misbehave(request: Request[String]): EitherT[M, HttpClientError, Response[String]] = {
    val data = Some(jobject(jfield("data", jarray())).renderCompact)
    val response = Response(200, "OK", data)
    EitherT(M point \/-(response))
  }

  private def empty(request: Request[String]): EitherT[M, HttpClientError, Response[String]] = {
    val response = Response(200, "OK", None)
    EitherT(M point \/-(response))
  }

  private def serverError(request: Request[String]): EitherT[M, HttpClientError, Response[String]] = {
    val response = Response(500, "Server Error", None)
    EitherT(M point \/-(response))
  }

  private val urlMap: Map[String, Request[String] => EitherT[M, HttpClientError, Response[String]]] = Map(
    "http://wrapper" -> (wrapper(_)),
    "http://echo" -> (echo(_)),
    "http://misbehave" -> (misbehave(_)),
    "http://empty" -> (empty(_)),
    "http://server-error" -> (serverError(_))
  )

  final class HttpClient(baseUrl: String) extends HttpClientLike {
    def execute(request: Request[String]): EitherT[M, HttpClientError, Response[String]] =
      urlMap get baseUrl map (_(request)) getOrElse {
        EitherT(M.point(-\/(HttpClientError.ConnectionError(Some(baseUrl), new java.io.IOException))))
      }
  }

  def HttpClient(baseUrl: String): HttpClient = new HttpClient(baseUrl)
}
