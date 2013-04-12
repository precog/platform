package com.precog.common
package services

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.http._
import blueeyes.core.service._

import scalaz.{Monad, Success}

object CORSHeaders {
  def genHeaders(
    methods: Seq[String] = Seq("GET", "POST", "OPTIONS", "DELETE", "PUT"),
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
  def allowOrigin[A, B](value: String, executor: ExecutionContext)(delegateService: HttpService[A, Future[HttpResponse[B]]]): HttpService[A, Future[HttpResponse[B]]] =
    new DelegatingService[A, Future[HttpResponse[B]], A, Future[HttpResponse[B]]] {
      private val corsHeaders = CORSHeaders.genHeaders(origin = value)
      val delegate = delegateService
      val service = (r: HttpRequest[A]) => r.method match {
        case HttpMethods.OPTIONS => Success(Promise.successful(HttpResponse(headers = corsHeaders))(executor))
        case _ => delegateService.service(r).map { _.map {
          case resp @ HttpResponse(_, headers, _, _) => resp.copy(headers = headers ++ corsHeaders)
        } }
      }

      val metadata = delegateService.metadata
    }
}
