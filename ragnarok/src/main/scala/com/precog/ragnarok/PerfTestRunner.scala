package com.precog.ragnarok

import scalaz._


// TODO: The Timer should just be a member.

trait PerfTestRunner[M[+_], T] {
  import scalaz.syntax.monad._
  import scalaz.syntax.monoid._
  import scalaz.syntax.copointed._
  import scalaz.std.option._

  implicit def M: Monad[M]

  /** Result type of running an eval. */
  type Result


  /** Evaluate a Quirrel query. */
  def eval(query: String): M[Result]


  val timer: Timer[T]

  import timer._


  type RunResult[A] = Tree[(PerfTest, A, Option[(T, T)])]


  private def fill[A](run: RunResult[A]): RunResult[A] = run match {
    case n @ Tree.Node((_: RunQuery, _, _), _) => n
    case Tree.Node((test, a, _), children) =>
      val kids = children map (fill(_))
      val t = kids.foldLeft(None: Option[(T, T)]) {
        case (acc, Tree.Node((_, _, t), _)) =>
          acc |+| t
      }
      Tree.node((test, a, t), kids)
  }

  private def merge[A: Monoid](run: RunResult[A], f: Option[(T, T)] => A): Tree[(PerfTest, A)] = {
    fill(run) match { case Tree.Node((test, a, time), children) =>
      Tree.node((test, a |+| f(time)), children map (merge(_, f)))
    }
  }

  def runAll[A](test: Tree[PerfTest], n: Int)(f: Option[(T, T)] => A)(implicit
    A: Monoid[A], M: Copointed[M]) = runAllM(test, n)(f).copoint

  /**
   * Runs `test` `n` times, merging the times for queries together by converting
   * the times to `A`s, then appending them.
   */
  def runAllM[A: Monoid](test: Tree[PerfTest], n: Int)(f: Option[(T, T)] => A) = {
    require(n > 0)

    (1 to n).foldLeft((test map (_ -> Monoid[A].zero)).pure[M]) { (acc, _) =>
      acc flatMap (runM(_)) map (merge(_, f))
    }
  }

  def runM[A](test: Tree[(PerfTest, A)]): M[RunResult[A]] = {
    test match {
      case Tree.Node((test @ RunQuery(q), a), _) =>
        timeQuery(q) map { case (t, _) =>
          Tree.leaf((test, a, Some(t)))
        }

      case Tree.Node((RunSequential, a), tests) =>
        tests.foldLeft(List[RunResult[A]]().pure[M]) { (acc, test) =>
            acc flatMap { rs =>
              runM(test) map (_ :: rs)
            }
        } map { children =>
          Tree.node((RunSequential, a, None), children.reverse.toStream)
        }

      case Tree.Node((RunConcurrent, a), tests) =>
        (tests map (runM(_))).foldLeft(List[RunResult[A]]().pure[M]) { (acc, run) =>
          acc flatMap { rs =>
            run map { _ :: rs }
          }
        } map { children =>
          Tree.node((RunConcurrent, a, None), children.reverse.toStream)
        }

      case Tree.Node((g: Group, a), tests) =>
        // tests really should only have size 1 in this case...
        runM(Tree.node((RunConcurrent, a), tests)) map {
          case Tree.Node((_, a, t), tests) =>
            Tree.node((g, a, t), tests)
        }
    }
  }

  private def time[A](f: => A): ((T, T), A) = {
    val start = now()
    val result = f
    timeSpan(start, now()) -> result
  }

  private def timeQuery(q: String): M[((T, T), Result)] = {
    val start = now()
    eval(q) map (timeSpan(start, now()) -> _)
  }
}


class MockPerfTestRunner[M[+_]](evalTime: => Int)(implicit val M: Monad[M]) extends PerfTestRunner[M, Long] {
  import scalaz.syntax.monad._

  type Result = Unit

  val timer = SimpleTimer

  def eval(query: String): M[Result] = {
    (()).pure[M] map { _ =>
      Thread.sleep(evalTime)
      ()
    }
  }
}

//
