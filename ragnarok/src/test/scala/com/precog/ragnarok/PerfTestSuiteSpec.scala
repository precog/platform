package com.precog.ragnarok

import org.specs2.mutable.Specification

import scalaz._
import scalaz.syntax.equal._
import scalaz.std.anyVal._


class PerfTestSuiteSpec extends Specification {
  implicit def perfTestEq = Equal.equalA[PerfTest]

  implicit def treeEq[A: Equal] = new Equal[Tree[A]] {
    def equal(a: Tree[A], b: Tree[A]): Boolean =
      Equal[A].equal(a.rootLabel, b.rootLabel) &&
        a.subForest.size == b.subForest.size &&
        ((a.subForest zip b.subForest) forall (equal _).tupled)
  }

  object ex extends PerfTestSuite {
    "a" := {
      "b" := concurrently {
        query("1")
        query("2")
      }
      query("3")
    }
  }

  val exInnerTest = Tree.node(RunSequential, Stream(
      Tree.node(Group("a"), Stream(
        Tree.node(RunSequential, Stream(
          Tree.node(Group("b"), Stream(
            Tree.node(RunConcurrent, Stream(
              Tree.leaf[PerfTest](RunQuery("1")),
              Tree.leaf[PerfTest](RunQuery("2"))
            ))
          )),
          Tree.leaf[PerfTest](RunQuery("3"))
        ))
      ))
    ))


  "the DSL" should {
    "create an initial group based on the class" in {
      object weird extends PerfTestSuite {
        query("1")
        query("2")
      }

      weird.test must beLike {
        case Tree.Node(Group(name), _) =>
          name must contain("weird")
      }
    }

    "faithfully reprsent the test suite" in {
      ex.test.subForest must have size(1)

      treeEq[PerfTest].equal(ex.test.subForest.head, exInnerTest) must beTrue
    }
  }

  "the test selector" should {
    "select whole tree if root is true" in {
      val t = ex select ((_, _) => true)
      t must have size(1)
      treeEq[PerfTest].equal(t.head, ex.test)
    }

    "selecting multiple disjoint tests gives list" in {
      val ts = ex select {
        case (_, _: RunQuery) => true
        case (_, _) => false
      }

      ts must haveTheSameElementsAs(List(
        Tree.leaf[PerfTest](RunQuery("1")),
        Tree.leaf[PerfTest](RunQuery("2")),
        Tree.leaf[PerfTest](RunQuery("3"))
      )) ^^ (treeEq[PerfTest].equal(_, _))
  }
  }
}

