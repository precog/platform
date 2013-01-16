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
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(CString("/file"))(line))(line),
          Const(CString("column"))(line))(line)

      val result = prettyPrint(input)
      
      val expected = 
        """|val line = Line(0, "")
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
      val line = Line(0, "")
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
        """|val line = Line(0, "")
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
      val line = Line(0, "")

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
        """|val line = Line(0, "")
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
