package com.precog.ragnarok

import scalaz.{ Ordering => _, _ }

import org.joda.time.{ Instant, Interval }


trait MetricSpace[A] {
  def distance(a: A, b: A): Double
}

object MetricSpace {
  def apply[A](implicit ms: MetricSpace[A]) = ms

  implicit object LongMetricSpace extends MetricSpace[Long] {
    def distance(a: Long, b: Long) = math.abs(b - a).toDouble
  }

  implicit object InstantMetricSpace extends MetricSpace[Instant] {
    def distance(a: Instant, b: Instant) = math.abs(b.getMillis - a.getMillis).toDouble
  }
}


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
trait SimpleTimer extends Timer[Long] {
  def now() = System.nanoTime()

  val TimeOrdering: Ordering[Long] = Ordering.Long
}


/**
 * Millisecond timer, implemented using Joda Time `Instant`s.
 */
trait JodaTimer extends Timer[Instant] {
  def now() = new Instant()

  val TimeOrdering: Ordering[Instant] = Ordering.Long.on[Instant](_.getMillis)
}



