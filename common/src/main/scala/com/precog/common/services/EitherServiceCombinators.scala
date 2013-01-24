package com.precog.common
package services

import blueeyes.core.http._
import blueeyes.core.service._

trait EitherServiceCombinators {
  def left[A, B, C](h: HttpService[Either[A, B], C]): HttpService[A, C] = h.contramap(Left(_))

  def right[A, B, C](h: HttpService[Either[A, B], C]): HttpService[B, C] = h.contramap(Right(_))
}

// vim: set ts=4 sw=4 et:
