package com.precog
package daze

import bytecode.RandomLibrary
import com.precog.yggdrasil._
import org.specs2.mutable._

object PrettyPrinterSpec extends Specification with PrettyPrinter with RandomLibrary with FNDummyModule {
  import dag._
  import instructions._
  import bytecode._

  "pretty printing" should {
    "format a trivial DAG" in {
      val line = Line(1, 1, "")

      val input =
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("/file"))(line))(line),
          Const(CString("column"))(line))(line)

      val result = prettyPrint(input)
      
      val expected = 
        """|val line = Line(1, 1, "")
           |
           |lazy val input =
           |  Join(DerefObject, CrossLeftSort,
           |    LoadLocal(
           |      Const(CString("/file"))(line)
           |    )(line),
           |    Const(CString("column"))(line)
           |  )(line)
           |""".stripMargin

      result must_== expected
    }

    "format a DAG with shared structure" in {
      val line = Line(1, 1, "")
      val file = dag.LoadLocal(Const(CString("/file"))(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          Join(DerefObject, CrossLeftSort, 
            file,
            Const(CString("time"))(line))(line),
          Join(DerefObject, CrossLeftSort,
            file,
            Const(CString("height"))(line))(line))(line)

      val result = prettyPrint(input)

      val expected =
        """|val line = Line(1, 1, "")
           |
           |lazy val node =
           |  LoadLocal(
           |    Const(CString("/file"))(line)
           |  )(line)
           |
           |lazy val input =
           |  Join(Add, IdentitySort,
           |    Join(DerefObject, CrossLeftSort,
           |      node,
           |      Const(CString("time"))(line)
           |    )(line),
           |    Join(DerefObject, CrossLeftSort,
           |      node,
           |      Const(CString("height"))(line)
           |    )(line)
           |  )(line)
           |""".stripMargin

      result must_== expected
    }

    "format a DAG containing a Split" in {
      val line = Line(1, 1, "")

      def clicks = dag.LoadLocal(Const(CString("/file"))(line))(line)

      lazy val input: dag.Split =
        dag.Split(
          dag.Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(DerefObject, CrossLeftSort,
                clicks,
                Const(CString("column0"))(line))(line))),
          Join(Add, IdentitySort,
            Join(DerefObject, CrossLeftSort,
              SplitParam(0)(input)(line),
              Const(CString("column1"))(line))(line),
            Join(DerefObject, CrossLeftSort,
              SplitGroup(1, clicks.identities)(input)(line),
              Const(CString("column2"))(line))(line))(line))(line)

      val result = prettyPrint(input)
      
      val expected =
        """|val line = Line(1, 1, "")
           |
           |lazy val node =
           |  LoadLocal(
           |    Const(CString("/file"))(line)
           |  )(line)
           |
           |lazy val input =
           |  Split(
           |    Group(1, 
           |      node,
           |      UnfixedSolution(0,
           |        Join(DerefObject, CrossLeftSort,
           |          node,
           |          Const(CString("column0"))(line)
           |        )(line)
           |      )
           |    ),
           |    Join(Add, IdentitySort,
           |      Join(DerefObject, CrossLeftSort,
           |        SplitParam(0)(input)(line),
           |        Const(CString("column1"))(line)
           |      )(line),
           |      Join(DerefObject, CrossLeftSort,
           |        SplitParam(1, Vector(LoadIds("/file")))(input)(line),
           |        Const(CString("column2"))(line)
           |      )(line)
           |    )(line)
           |  )(line)
           |""".stripMargin

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
