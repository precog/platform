package com.precog.common

import blueeyes.core.http._

import scalaz.Monad

object OptionHeaders {
  def apply[T, M[+_]](M: Monad[M]) = M.point(
    HttpResponse[T](headers = HttpHeaders(Seq("Allow" -> "GET,POST,OPTIONS",
      "Access-Control-Allow-Origin" -> "*",
      "Access-Control-Allow-Methods" -> "GET, POST, OPTIONS, DELETE",
      "Access-Control-Allow-Headers" -> "Origin, X-Requested-With, Content-Type, X-File-Name, X-File-Size, X-File-Type, X-Precog-Path, X-Precog-Service, X-Precog-Token, X-Precog-Uuid, Accept")))
  )
}
