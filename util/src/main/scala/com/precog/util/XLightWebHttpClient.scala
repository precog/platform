package com.precog.util

import java.net.URL
import java.util.concurrent.Future

import org.xlightweb._
import org.xlightweb.client.{HttpClient => XHttpClient, _}

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.bifunctor._

trait XLightWebHttpClientModule[M[+_]] extends HttpClientModule[M] {
  
  private def liftJUCFuture[A](f: Future[A]): M[A] = M point f.get // Ugh.
  
  def HttpClient(baseUrl: String): HttpClient = new HttpClient(baseUrl)
  
  class HttpClient(baseUrl: String) extends HttpClientLike {
    private val client = new XHttpClient

    private def fromTryCatch[A](req: Option[IHttpRequest])(f: => A): HttpClientError \/ A =
      { (t: Throwable) => HttpClientError.ConnectionError(req map (_.getRequestURI), t) } <-: \/.fromTryCatch(f)

    private def buildUrl(path: String): HttpClientError \/ URL = fromTryCatch(None) {
      val url0 = new URL(baseUrl)
      new URL(url0.getProtocol, url0.getHost, url0.getPort, url0.getPath + path)
    }

    private def buildRequest(request: Request[String]): HttpClientError \/ IHttpRequest = {
      buildUrl(request.path) map { url =>
        val req = request.method match {
          case HttpMethod.GET => new GetRequest(url.toString)
          case HttpMethod.POST =>
            request.body map { case Request.Body(contenType, body) =>
              new PostRequest(url.toString, contenType, body)
            } getOrElse new PostRequest(url.toString)
        }
        request.params foreach (req.setParameter(_: String, _: String)).tupled
        req
      }
    }

    private def execute0(request: IHttpRequest): EitherT[M, HttpClientError, IHttpResponse] =
      EitherT(fromTryCatch(Some(request))(liftJUCFuture(client.send(request))).sequence[M, IHttpResponse])

    def execute(request: Request[String]): EitherT[M, HttpClientError, Response[String]] = for {
      httpRequest <- EitherT(M point buildRequest(request))
      response <- execute0(httpRequest)
      body <- EitherT(M point fromTryCatch(Some(httpRequest))(Option(response.getBody) map (_.readString("UTF-8"))))
    } yield {
      Response(response.getStatus, response.getReason, body)
    }
  }
}
