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
package services

import akka.dispatch.Future

import blueeyes.core.http._
import blueeyes.core.service._

import scalaz.Monad

object CORSHeaders {
  def genHeaders(
    methods: Seq[String] = Seq("GET", "POST", "OPTIONS", "DELETE"),
    origin: String = "*",
    headers: Seq[String] = Seq("Origin", "X-Requested-With", "Content-Type", "X-File-Name", "X-File-Size", "X-File-Type", "X-Precog-Path", "X-Precog-Service", "X-Precog-Token", "X-Precog-Uuid", "Accept")): HttpHeaders = {

    HttpHeaders(Seq(
      "Allow" -> methods.mkString(","),
      "Access-Control-Allow-Origin" -> origin,
      "Access-Control-Allow-Methods" -> methods.mkString(","),
      "Access-Control-Allow-Headers" -> headers.mkString(",")))
  }

  val defaultHeaders = genHeaders()

  def apply[T, M[+_]](M: Monad[M]) = M.point(
    HttpResponse[T](headers = defaultHeaders)
  )
}

object CORSHeaderHandler {
  def allowOrigin[A, B](value: String)(service: HttpService[A, Future[HttpResponse[B]]]): HttpService[A, Future[HttpResponse[B]]] = service map { _.map {
    case resp @ HttpResponse(_, headers, _, _) => resp.copy(headers = headers ++ CORSHeaders.genHeaders(origin = value))
  } }
}
