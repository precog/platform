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
      val line = Line(0, "")

      val input =
        Join(line, DerefObject, CrossLeftSort,
          dag.LoadLocal(line, Const(line, CString("/file"))),
          Const(line, CString("column")))

      val result = prettyPrint(input)
      
      val expected = 
        """|val line = Line(0, "")
           |
           |lazy val input =
           |  Join(line, DerefObject, CrossLeftSort,
           |    LoadLocal(line,
           |      Const(line, CString("/file"))
           |    ),
           |    Const(line, CString("column"))
           |  )
           |""".stripMargin

      result must_== expected
    }

    "format a DAG with shared structure" in {
      val line = Line(0, "")
      val file = dag.LoadLocal(line, Const(line, CString("/file")))
      
      val input =
        Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            file,
            Const(line, CString("time"))),
          Join(line, DerefObject, CrossLeftSort,
            file,
            Const(line, CString("height"))))

      val result = prettyPrint(input)

      val expected =
        """|val line = Line(0, "")
           |
           |lazy val node =
           |  LoadLocal(line,
           |    Const(line, CString("/file"))
           |  )
           |
           |lazy val input =
           |  Join(line, Add, IdentitySort,
           |    Join(line, DerefObject, CrossLeftSort,
           |      node,
           |      Const(line, CString("time"))
           |    ),
           |    Join(line, DerefObject, CrossLeftSort,
           |      node,
           |      Const(line, CString("height"))
           |    )
           |  )
           |""".stripMargin

      result must_== expected
    }

    "format a DAG containing a Split" in {
      val line = Line(0, "")

      def clicks = dag.LoadLocal(line, Const(line, CString("/file")))

      lazy val input: dag.Split =
        dag.Split(line,
          dag.Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Const(line, CString("column0"))))),
          Join(line, Add, IdentitySort,
            Join(line, DerefObject, CrossLeftSort,
              SplitParam(line, 0)(input),
              Const(line, CString("column1"))),
            Join(line, DerefObject, CrossLeftSort,
              SplitGroup(line, 1, clicks.identities)(input),
              Const(line, CString("column2")))))

      val result = prettyPrint(input)
      
      val expected =
        """|val line = Line(0, "")
           |
           |lazy val node =
           |  LoadLocal(line,
           |    Const(line, CString("/file"))
           |  )
           |
           |lazy val input =
           |  Split(line,
           |    Group(1, 
           |      node,
           |      UnfixedSolution(line, 0,
           |        Join(line, DerefObject, CrossLeftSort,
           |          node,
           |          Const(line, CString("column0"))
           |        )
           |      )
           |    ),
           |    Join(line, Add, IdentitySort,
           |      Join(line, DerefObject, CrossLeftSort,
           |        SplitParam(line, 0)(input),
           |        Const(line, CString("column1"))
           |      ),
           |      Join(line, DerefObject, CrossLeftSort,
           |        SplitParam(line, 1, Vector(LoadIds("/file"))(input),
           |        Const(line, CString("column2"))
           |      )
           |    )
           |  )
           |""".stripMargin

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
