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
