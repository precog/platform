package com.precog.ragnarok

import scalaz.{ Ordering => _, _ }

import org.joda.time.{ Instant, Interval }


trait Timer[T] {
  import scala.math.Ordering.Implicits._

  /** Returns the current time. */
  def now(): T

  def timeSpan(start: T, end: T) = (start, end)

  implicit def TimeOrdering: Ordering[T]

  implicit def TimeSpanSemigroup: Semigroup[(T, T)] = new Semigroup[(T, T)] {
    def append(a: (T, T), b: => (T, T)) = (a._1 min b._1, a._2 max b._2)
  }
}


/**
 * Nanosecond timer, implemented using `System.nanoTime()`.
 */
object SimpleTimer extends Timer[Long] {
  def now() = System.nanoTime()

  val TimeOrdering: Ordering[Long] = Ordering.Long
}


/**
 * Millisecond timer, implemented using Joda Time `Instant`s.
 */
object JodaTimer extends Timer[Instant] {
  def now() = new Instant()

  val TimeOrdering: Ordering[Instant] = Ordering.Long.on[Instant](_.getMillis)
}



