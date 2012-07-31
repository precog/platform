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

import scala.annotation.tailrec

import scalaz._
import scalaz.std.option._

import java.util.concurrent.TimeUnit


case class RunConfig[M[+_], T](
    runner: PerfTestRunner[M, T],
    runs: Int,
    outliers: Double) {
  def tails: Int = (runs * outliers).toInt
}


final class PerfTestStatsPrettyPrinter(result: Tree[(PerfTest, Option[Statistics])]) {

  private def prettyStats(stats: Option[Statistics], toMillis: Double => Double): String =
    stats map { s => "%.1f ms" format toMillis(s.mean) } getOrElse ""
    
  def prettyStats(toMillis: Double => Double = identity): String = {
    def lines(test: Tree[(PerfTest, Option[Statistics])]): List[String] = {
      test match {
        case Tree.Node((Group(name), _), kids) =>
          name :: (kids.toList flatMap (lines(_)))

        case Tree.Node((RunSequential, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" + " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) :+ (" ` " + prettyStats(s, toMillis))

        case Tree.Node((RunConcurrent, s), kids) =>
          (kids.toList map (lines(_)) flatMap {
            case head :: tail =>
              (" * " + head) :: (tail map (" | " + _))
            case Nil => Nil
          }) :+ (" ` " + prettyStats(s, toMillis))

        case Tree.Node((RunQuery(q), s), kids) =>
          ("-> " + q) :: ("   " + prettyStats(s, toMillis)) :: Nil
      }
    }

    lines(result) mkString "\n"
  }
}

object PerfTestPrettyPrinters {
  implicit def statsPrinter(result: Tree[(PerfTest, Option[Statistics])]) =
    new PerfTestStatsPrettyPrinter(result)
}


trait PerfTestSuite {
  
  private var tests: List[Tree[PerfTest]] = Nil

  /** Returns the top-level test for this suite. */
  def test: Tree[PerfTest] =
    Tree.node(Group(getClass.getName), Stream(Tree.node(RunSequential,
      tests.reverse.toStream)))


  /**
   * Any tests created while running `f` will be given to `g` to consolidate
   * into 1 test.
   */
  protected def collect(f: => Any)(g: List[Tree[PerfTest]] => Tree[PerfTest]) {
    val state = tests
    tests = Nil
    f
    val t = g(tests.reverse)
    tests = t :: state
  }


  final class GroupedDef(name: String) {
    def `:=`(f: => Any) = collect(f) {
      case (t @ Tree.Node(RunConcurrent, _)) :: Nil =>
        Tree.node(Group(name), Stream[Tree[PerfTest]](t))

      case kids =>
        Tree.node(Group(name), Stream(Tree.node(RunSequential, kids.toStream)))
    }
  }

  implicit def GroupedDef(name: String) = new GroupedDef(name)

  def concurrently(f: => Any) {
    collect(f)(kids => Tree.node(RunConcurrent, kids.toStream))
  }

  def query(q: String) {
    tests = Tree.leaf[PerfTest](RunQuery(q)) :: tests
  }


  def select(pred: (List[String], PerfTest) => Boolean): List[Tree[PerfTest]] =
    selectTest(test, pred)


  def run[M[+_]: Copointed, T: MetricSpace](config: RunConfig[M, T]) =
    config.runner.runAll(test, config.runs) {
      case None => None
      case Some((a, b)) =>
        Some(Statistics(MetricSpace[T].distance(a, b), tails = config.tails))
    }


  def main(args: Array[String]) {
  }


  /**
   * Selects a test based on paths, using select to determine which sub-trees
   * should be included.
   */
  private def selectTest(test: Tree[PerfTest], select: (List[String], PerfTest) => Boolean): List[Tree[PerfTest]] = {

    @tailrec
    def find(loc: TreeLoc[PerfTest], path: List[String],
        matches: List[Tree[PerfTest]],
        retreat: Boolean = false): List[Tree[PerfTest]] = {

      if (retreat) {

        val p = loc.tree match {
          case Tree.Node(Group(_), _) => path.tail
          case _ => path
        }

        loc.right match {
          case Some(loc) =>
            find(loc, p, matches)

          case None =>
            loc.parent match {
              case Some(loc) =>
                find(loc, p, matches, retreat = true)

              case _ =>
                matches
            }
        }

      } else {

        val p = loc.tree match {
          case Tree.Node(Group(name), _) => name :: path
          case _ => path
        }

        if (select(p.reverse, loc.tree.rootLabel)) {
          find(loc, p, loc.tree :: matches, retreat = true)
        } else {
          loc.firstChild match {
            case Some(loc) =>
              find(loc, p, matches)

            case None =>
              find(loc, p, matches, retreat = true)
          }
        }
      }
    }

    find(test.loc, Nil, Nil)
  }
}
