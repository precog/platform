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

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

object ServiceHandlerUtil {
  def forbidden(message: String): HttpResponse[JValue] =
    HttpResponse[JValue](HttpStatus(Forbidden), content = Some(JString(message)))

  def badRequest(message: String, details: Option[String] = None): HttpResponse[JValue] = 
    HttpResponse[JValue](HttpStatus(BadRequest, message), content = Some(jobject(jfield("error", details getOrElse message))))
  
  def notFound(message: String): HttpResponse[JValue] = 
    HttpResponse[JValue](HttpStatus(NotFound), content = Some(JString(message)))

  def ok[A: Decomposer](content: Option[A]): HttpResponse[JValue] = 
    HttpResponse[JValue](OK, content = content.map(_.serialize))

  def created[A: Decomposer](content: Option[A]): HttpResponse[JValue] = 
    HttpResponse[JValue](Created, content = content.map(_.serialize))

  def noContent: HttpResponse[JValue] =
    HttpResponse[JValue](HttpStatus(NoContent))

  def serverError(message: String, details: Option[String] = None): HttpResponse[JValue] = 
    HttpResponse(HttpStatus(InternalServerError, message), content = Some(jobject(jfield("error", details getOrElse message))))
}

// vim: set ts=4 sw=4 et:
