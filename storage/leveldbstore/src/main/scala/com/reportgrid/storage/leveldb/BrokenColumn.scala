package com.reportgrid.storage

import scalaz.{Ordering => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.iteratee.Input._
import IterateeT._
import scalaz.Scalaz._

class OwMyCycles[T](name : String, dataDir : String)(implicit order : Ordering[T]) {
  def breakage[F[_] : MonadIO, A](v : T) = v
}
