package com.precog.ragnarok

import org.joda.time.{ Instant, Interval }


// Scalaz has a MetricSpace, but its raison d'etre is for Lev. dist and can
// only be used for integer distances.

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




