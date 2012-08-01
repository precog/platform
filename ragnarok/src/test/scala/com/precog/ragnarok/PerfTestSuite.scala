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

import com.weiglewilczek.slf4s.Logging


trait PerfTestSuite extends Logging {
  
  private var tests: List[Tree[PerfTest]] = Nil

  def suiteName: String = getClass.getName

  /** Returns the top-level test for this suite. */
  def test: Tree[PerfTest] =
    Tree.node(Group(suiteName), Stream(Tree.node(RunSequential,
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

  def include(suite: PerfTestSuite) {
    tests = suite.test :: tests
  }


  def select(pred: (List[String], PerfTest) => Boolean): Option[Tree[PerfTest]] =
    selectTest(test, pred)


  def run[M[+_]: Copointed, T: MetricSpace](test: Tree[PerfTest] = test,
      runner: PerfTestRunner[M, T], runs: Int = 60, outliers: Double = 0.05) = {
    val tails = (runs * (outliers / 2)).toInt

    runner.runAll(test, runs) {
      case None => None
      case Some((a, b)) =>
        Some(Statistics(MetricSpace[T].distance(a, b), tails = tails))
    }
  }


  def main(args: Array[String]) {
    import akka.actor.ActorSystem
    import akka.dispatch.{ Future, ExecutionContext, Await }
    import blueeyes.bkka.AkkaTypeClasses._
    import PerfTestPrettyPrinters._

    RunConfig.fromCommandLine(args.toList) match {
      case Failure(errors) =>
        System.err.println("Error parsing command lines:")
        errors.list foreach { msg => System.err.println("\t" + msg) }
        System.err.println()

      case Success(config) =>
        val actorSystem = ActorSystem("perfTestingActorSystem")
        try {
          implicit val execContext = ExecutionContext.defaultExecutionContext(actorSystem)

          val runner = new JsonPerfTestRunner[Future, Long](SimpleTimer, _optimize = true, _userUID = "dummy")

          implicit val futureIsCopointed = new Copointed[Future] {
            def map[A, B](m: Future[A])(f: A => B) = m map f
            def copoint[A](f: Future[A]) = Await.result(f, runner.yggConfig.maxEvalDuration)
          }

          select(config.select getOrElse ((_, _) => true)) foreach { test =>
            if (config.dryRuns > 0) {
              run(test = test, runner = runner, runs = config.dryRuns, outliers = config.outliers)
            }

            println(run(test = test, runner = runner, runs = config.runs, outliers = config.outliers).prettyStats(_ / 1000000))
          }

        } finally {
          actorSystem.shutdown()
        }
    }
  }


  /**
   * Selects a test based on paths, using select to determine which sub-trees
   * should be included.
   */
  private def selectTest(test: Tree[PerfTest], select: (List[String], PerfTest) => Boolean): Option[Tree[PerfTest]] = {

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

    test.subForest.headOption flatMap { root =>
      find(root.loc, Nil, Nil) match {
        case Nil => None
        case tests => Some(Tree.node(Group(suiteName), tests.toStream))
      }
    }
  }
}
