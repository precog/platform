package com.precog
package daze

import bytecode.RandomLibrary

import org.specs2.mutable._

object PrettyPrinterSpec extends Specification with PrettyPrinter with RandomLibrary {
  import dag._
  import instructions.{
    Line,
    Add,
    DerefObject,
    PushString, PushNum
  }
  import bytecode._

  "pretty printing" should {
    "format a trivial DAG" in {
      val line = Line(0, "")

      val input =
        Join(line, DerefObject, CrossLeftSort,
          LoadLocal(line, Root(line, PushString("/file"))),
          Root(line, PushString("column")))

      val result = prettyPrint(input)
      
      val expected = 
        """|lazy val input =
           |  Join(line, DerefObject, CrossLeftSort,
           |    LoadLocal(line,
           |      Root(line, PushString("/file"))
           |    ),
           |    Root(line, PushString("column"))
           |  )
           |""".stripMargin

      result must_== expected
    }

    "format a DAG with shared structure" in {
      val line = Line(0, "")
      val file = LoadLocal(line, Root(line, PushString("/file"))) 
      
      val input =
        Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            file,
            Root(line, PushString("time"))),
          Join(line, DerefObject, CrossLeftSort,
            file,
            Root(line, PushString("height"))))

      val result = prettyPrint(input)

      val expected =
        """|lazy val node =
           |  LoadLocal(line,
           |    Root(line, PushString("/file"))
           |  )
           |
           |lazy val input =
           |  Join(line, Add, IdentitySort,
           |    Join(line, DerefObject, CrossLeftSort,
           |      node,
           |      Root(line, PushString("time"))
           |    ),
           |    Join(line, DerefObject, CrossLeftSort,
           |      node,
           |      Root(line, PushString("height"))
           |    )
           |  )
           |""".stripMargin

      result must_== expected
    }

    "format a DAG containing a Split" in {
      val line = Line(0, "")

      def clicks = LoadLocal(line, Root(line, PushString("/file")))

      lazy val input: Split =
        Split(line,
          Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Root(line, PushString("column0"))))),
          Join(line, Add, IdentitySort,
            Join(line, DerefObject, CrossLeftSort,
              SplitParam(line, 0)(input),
              Root(line, PushString("column1"))),
            Join(line, DerefObject, CrossLeftSort,
              SplitGroup(line, 1, clicks.provenance)(input),
              Root(line, PushString("column2")))))

      val result = prettyPrint(input)
      
      val expected =
        """|lazy val node =
           |  LoadLocal(line,
           |    Root(line, PushString("/file"))
           |  )
           |
           |lazy val input =
           |  Split(line,
           |    Group(1, 
           |      node,
           |      UnfixedSolution(line, 0,
           |        Join(line, DerefObject, CrossLeftSort,
           |          node,
           |          Root(line, PushString("column0"))
           |        )
           |      )
           |    ),
           |    Join(line, Add, IdentitySort,
           |      Join(line, DerefObject, CrossLeftSort,
           |        SplitParam(line, 0)(input),
           |        Root(line, PushString("column1"))
           |      ),
           |      Join(line, DerefObject, CrossLeftSort,
           |        SplitParam(line, 1, Vector(StaticProvenance("/file"))(input),
           |        Root(line, PushString("column2"))
           |      )
           |    )
           |  )
           |""".stripMargin

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
