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
          dag.LoadLocal(line, Root(line, CString("/file"))),
          Root(line, CString("column")))

      val result = prettyPrint(input)
      
      val expected = 
        """|val line = Line(0, "")
           |
           |lazy val input =
           |  Join(line, DerefObject, CrossLeftSort,
           |    LoadLocal(line,
           |      Root(line, CString("/file"))
           |    ),
           |    Root(line, CString("column"))
           |  )
           |""".stripMargin

      result must_== expected
    }

    "format a DAG with shared structure" in {
      val line = Line(0, "")
      val file = dag.LoadLocal(line, Root(line, CString("/file"))) 
      
      val input =
        Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            file,
            Root(line, CString("time"))),
          Join(line, DerefObject, CrossLeftSort,
            file,
            Root(line, CString("height"))))

      val result = prettyPrint(input)

      val expected =
        """|val line = Line(0, "")
           |
           |lazy val node =
           |  LoadLocal(line,
           |    Root(line, CString("/file"))
           |  )
           |
           |lazy val input =
           |  Join(line, Add, IdentitySort,
           |    Join(line, DerefObject, CrossLeftSort,
           |      node,
           |      Root(line, CString("time"))
           |    ),
           |    Join(line, DerefObject, CrossLeftSort,
           |      node,
           |      Root(line, CString("height"))
           |    )
           |  )
           |""".stripMargin

      result must_== expected
    }

    "format a DAG containing a Split" in {
      val line = Line(0, "")

      def clicks = dag.LoadLocal(line, Root(line, CString("/file")))

      lazy val input: dag.Split =
        dag.Split(line,
          dag.Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Root(line, CString("column0"))))),
          Join(line, Add, IdentitySort,
            Join(line, DerefObject, CrossLeftSort,
              SplitParam(line, 0)(input),
              Root(line, CString("column1"))),
            Join(line, DerefObject, CrossLeftSort,
              SplitGroup(line, 1, clicks.identities)(input),
              Root(line, CString("column2")))))

      val result = prettyPrint(input)
      
      val expected =
        """|val line = Line(0, "")
           |
           |lazy val node =
           |  LoadLocal(line,
           |    Root(line, CString("/file"))
           |  )
           |
           |lazy val input =
           |  Split(line,
           |    Group(1, 
           |      node,
           |      UnfixedSolution(line, 0,
           |        Join(line, DerefObject, CrossLeftSort,
           |          node,
           |          Root(line, CString("column0"))
           |        )
           |      )
           |    ),
           |    Join(line, Add, IdentitySort,
           |      Join(line, DerefObject, CrossLeftSort,
           |        SplitParam(line, 0)(input),
           |        Root(line, CString("column1"))
           |      ),
           |      Join(line, DerefObject, CrossLeftSort,
           |        SplitParam(line, 1, Vector(LoadIds("/file"))(input),
           |        Root(line, CString("column2"))
           |      )
           |    )
           |  )
           |""".stripMargin

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
