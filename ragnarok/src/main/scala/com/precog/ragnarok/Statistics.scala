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

import scalaz._
import scalaz.std.option._


case class Stats private (
  tails: Int,
  min: List[Double],
  max: List[Double],
  m: Double,
  vn: Double,
  n: Int) {

  def +(that: Stats): Stats = Stats.semigroup.append(this, that)

  // Calculates the mean, variance, and count without outliers.
  private lazy val meanVarCount: (Double, Double, Int) =
    (min.reverse.tail ++ max.tail).foldLeft((m, vn, n)) {
      case ((m, vn, n), x) =>
        val mprev = m + (m - x) / (n - 1)
        val sprev = vn - (x - mprev) * (x - m)
        (mprev, sprev, n - 1)
    } match {
      case (m, vn, n) => (m, vn / n, n)
    }

  def mean: Double = meanVarCount._1

  // variance is wrong... outliers or no...
  def variance: Double = meanVarCount._2

  def stdDev: Double = math.sqrt(variance)

  def count: Int = meanVarCount._3
}

object Stats {
  def apply(x: Double, tails: Int = 0): Stats =
    Stats(tails, x :: Nil, x :: Nil, x, 0.0, 1)

  implicit object semigroup extends Semigroup[Stats] { 
    def append(x: Stats, _y: => Stats): Stats = {
      val y = _y

      val z_m = x.m + (y.n / (x.n + y.n).toDouble) * (y.m - x.m)
      val z_vn = x.vn + y.n * (y.m - x.m) * (y.m - z_m)
      val z_tails = x.tails min y.tails

      Stats(z_tails,
        (x.min ++ y.min).sorted take (z_tails + 1),
        (x.max ++ y.max).sorted takeRight (z_tails + 1),
        z_m, z_vn, x.n + y.n)
    }
  }
}


trait RunnerDSL[M[+_], T] { self =>
  def runs: Int
  def outliers: Double
  def runner: PerfTestRunner[M, T]

  def run(test: Tree[PerfTest])(implicit T: MetricSpace[T], M: Copointed[M]) =
    runner.runAll(test, runs) {
      case None => None
      case Some((a, b)) =>
        Some(Stats(T.distance(a, b), tails = (runs * outliers).toInt))
    }

  final class PerfTestOps(test: Tree[PerfTest])(implicit T: MetricSpace[T], M: Copointed[M]) {
    def run: Tree[(PerfTest, Option[Stats])] = self.run(test)
  }

  implicit def PerfTestOps(test: Tree[PerfTest])(implicit
    T: MetricSpace[T], M: Copointed[M]) = new PerfTestOps(test)
}

trait DefinitionDSL {
  def query(q: String) = Tree.leaf[PerfTest](RunQuery(q))

  def sequence(tests: Tree[PerfTest]*) =
    Tree.node[PerfTest](RunSequential, tests.toStream)

  def concurrent(tests: Tree[PerfTest]*) =
    Tree.node[PerfTest](RunConcurrent, tests.toStream)
}

trait MutableDefinitionDSL extends DefinitionDSL {
  private var tests: List[Tree[PerfTest]] = Nil

  class ConcurrentDef(t1: Tree[PerfTest]) {
    def `with`(t2: Tree[PerfTest]) = t1 match {
      case Tree.Node(RunConcurrent, kids) =>
        Tree.node(RunConcurrent, kids :+ t2)

      case _ =>
        Tree.node(RunConcurrent, Stream(t1, t2))
    }
  }

  implicit def concurrentDef(t1: Tree[PerfTest]) = new ConcurrentDef(t1)
}
