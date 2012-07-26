package com.precog.ragnarock

import scalaz._

import org.joda.time.{ Instant, Interval }


trait Timer {

  /** A single instance in time. */
  type Time

  /** A span between 2 instances in time. */
  type TimeSpan

  /** Returns the current time. */
  def now(): Time

  /** Returns the time span between 2 times. */
  def duration(start: Time, end: Time): TimeSpan

  /**
   * The product of 2 `TimeSpan`s should be the extents of the union of both.
   * Since there is no identity in this case, `TimeSpan` is a `Semigroup`.
   * Wrap it in an `Option` to make it a `Monoid`.
   */
  implicit def TimeSpanSemigroup: Semigroup[TimeSpan]

  // TODO: We can create the semigroup in here if we have a way to extract the
  // end points from a TimeSpan and have an ordering for Time.
}


trait SimpleTimer extends Timer {
  type Time = Long
  type TimeSpan = (Time, Time)

  def now(): Time = System.nanoTime()

  def duration(start: Time, end: Time): TimeSpan = (start, end)

  val TimeSpanSemigroup = new Semigroup[TimeSpan] {
    def append(a: TimeSpan, b: => TimeSpan): TimeSpan =
      (a._1 min b._1, a._2 max b._2)
  }
}



trait JodaTimer extends Timer {
  type Time = Instant
  type TimeSpan = Interval

  def now(): Time = new Instant()
  def duration(start: Time, end: Time) = new Interval(start, end)

  val TimeSpanSemigroup = new Semigroup[TimeSpan] {
    def append(a: TimeSpan, b: => TimeSpan) = {
      val start = if (b.getStart isBefore a.getStart) b.getStart else a.getStart
      val end = if (a.getEnd isBefore b.getEnd) b.getEnd else a.getEnd
      new Interval(start, end)
    }
  }
}



