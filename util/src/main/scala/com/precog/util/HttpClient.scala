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

  sealed trait HttpClientError {
    def userMessage: String
  }

  object HttpClientError {
    case class ConnectionError(url: Option[String], cause: Throwable) extends HttpClientError {
      def userMessage = url map ("Error connecting to " + _) getOrElse "Invalid URL"
    }
    case class NotOk(code: Int, message: String) extends HttpClientError {
      def userMessage = "Bad response from upstream server: %d %s" format (code, message)
    }
    case object EmptyBody extends HttpClientError {
      def userMessage = "Server returned OK, but with no data"
    }
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
