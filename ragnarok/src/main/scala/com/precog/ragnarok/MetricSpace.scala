/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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




