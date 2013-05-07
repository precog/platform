package com.precog.util

import akka.dispatch.Future
import scalaz._

/**
 * Very stupid-simple HTTP client.  If we need something more powerful, speak
 * to the management.
 */
trait HttpClientModule[M[+_]] {
  implicit def M: Monad[M]
  
  type HttpClient <: HttpClientLike
  
  def HttpClient(host: String): M[HttpClient]
  
  trait HttpClientLike {
    def get(path: String, params: (String, String)*): M[Option[String]]
    def post(path: String, contentType: String, body: String, params: (String, String)*): M[Option[String]]
  }
}
