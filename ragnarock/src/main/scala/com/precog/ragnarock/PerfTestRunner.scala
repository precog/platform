package com.precog.ragnarock

import scalaz._
// import blueeyes.util.Clock


trait PerfTestRunner[M[+_]] { self: Timer =>
  import scalaz.syntax.monad._
  import scalaz.syntax.copointed._

  implicit def M: Monad[M]

  /** Result type of running an eval. */
  type Result


  /** Evaluate a Quirrel query. */
  def eval(query: String): M[Result]


  def run(test: PerfTest)(implicit M: Copointed[M]) = runM(test).copoint

  def runM(test: PerfTest): M[PerfTestResult[TimeSpan]] = test match {
    case TimeQuery(query) =>
      timeQuery(query) map { case (t, _) => QueryResult(query, t) }

    case GroupedTest(name, test) =>
      runM(test) map {
        case GroupedResult(None, rs) =>
          GroupedResult(Some(name), rs)

        case r @ (GroupedResult(_, _) | QueryResult(_, _)) =>
          GroupedResult(Some(name), r :: Nil)
      }

    case ConcurrentTests(tests) =>
      (tests map (runM(_))).foldLeft(GroupedResult[TimeSpan](None, Nil).pure[M]) { (acc, testRun) =>
        acc flatMap { case GroupedResult(_, rs) =>
          testRun map { r => GroupedResult(None, r :: rs) }
        }
      } map {
        case GroupedResult(_, rs) => GroupedResult(None, rs.reverse)
      }

    case SequenceTests(tests) =>
      tests.foldLeft(GroupedResult[TimeSpan](None, Nil).pure[M]) { (result, test) =>
        result flatMap { case GroupedResult(_, rs) =>
          runM(test) map { r => GroupedResult(None, r :: rs) }
        }
      } map {
        case GroupedResult(_, rs) => GroupedResult(None, rs.reverse)
      }
  }


  private def time[A](f: => A): (TimeSpan, A) = {
    val start = now()
    val result = f
    duration(start, now()) -> result
  }

  private def timeQuery(q: String): M[(TimeSpan, Result)] = {
    val start = now()
    eval(q) map (duration(start, now()) -> _)
  }
}


class MockPerfTestRunner[M[+_]](evalTime: => Int)(implicit val M: Monad[M]) extends PerfTestRunner[M] with SimpleTimer {
  import scalaz.syntax.monad._

  type Result = Unit

  def eval(query: String): M[Result] = {
    Thread.sleep(evalTime)
    (()).pure[M]
  }
}


// 
