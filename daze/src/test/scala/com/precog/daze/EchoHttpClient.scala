package com.precog.daze

import com.precog.util.HttpClientModule

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
