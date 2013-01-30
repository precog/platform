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

import blueeyes.json._

object PrettyPrinterSpecs extends Specification with PrettyPrinter with FNDummyModule {
  import dag._
  import instructions._
  import bytecode._

  type Lib = RandomLibrary
  object library extends RandomLibrary

  "pretty printing" should {
    "format a trivial DAG" in {
      val line = Line(1, 1, "")

      val input =
        Join(DerefObject, CrossLeftSort,
          dag.LoadLocal(Const(JString("/file"))(line))(line),
          Const(JString("column"))(line))(line)

      val result = prettyPrint(input)
      
      val expected = 
        """|val line = Line(1, 1, "")
           |
           |lazy val input =
           |  Join(DerefObject, CrossLeftSort,
           |    LoadLocal(
           |      Const(JString("/file"))(line)
           |    )(line),
           |    Const(JString("column"))(line)
           |  )(line)
           |""".stripMargin

      result must_== expected
    }

    "format a DAG with shared structure" in {
      val line = Line(1, 1, "")
      val file = dag.LoadLocal(Const(JString("/file"))(line))(line)
      
      val input =
        Join(Add, IdentitySort,
          Join(DerefObject, CrossLeftSort, 
            file,
            Const(JString("time"))(line))(line),
          Join(DerefObject, CrossLeftSort,
            file,
            Const(JString("height"))(line))(line))(line)

      val result = prettyPrint(input)

      val expected =
        """|val line = Line(1, 1, "")
           |
           |lazy val node =
           |  LoadLocal(
           |    Const(JString("/file"))(line)
           |  )(line)
           |
           |lazy val input =
           |  Join(Add, IdentitySort,
           |    Join(DerefObject, CrossLeftSort,
           |      node,
           |      Const(JString("time"))(line)
           |    )(line),
           |    Join(DerefObject, CrossLeftSort,
           |      node,
           |      Const(JString("height"))(line)
           |    )(line)
           |  )(line)
           |""".stripMargin

      result must_== expected
    }

    "format a DAG containing a Split" in {
      val line = Line(1, 1, "")

      def clicks = dag.LoadLocal(Const(JString("/file"))(line))(line)

      lazy val input: dag.Split =
        dag.Split(
          dag.Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(DerefObject, CrossLeftSort,
                clicks,
                Const(JString("column0"))(line))(line))),
          Join(Add, IdentitySort,
            Join(DerefObject, CrossLeftSort,
              SplitParam(0)(input)(line),
              Const(JString("column1"))(line))(line),
            Join(DerefObject, CrossLeftSort,
              SplitGroup(1, clicks.identities)(input)(line),
              Const(JString("column2"))(line))(line))(line))(line)

      val result = prettyPrint(input)
      
      val expected =
        """|val line = Line(1, 1, "")
           |
           |lazy val node =
           |  LoadLocal(
           |    Const(JString("/file"))(line)
           |  )(line)
           |
           |lazy val input =
           |  Split(
           |    Group(1, 
           |      node,
           |      UnfixedSolution(0,
           |        Join(DerefObject, CrossLeftSort,
           |          node,
           |          Const(JString("column0"))(line)
           |        )(line)
           |      )
           |    ),
           |    Join(Add, IdentitySort,
           |      Join(DerefObject, CrossLeftSort,
           |        SplitParam(0)(input)(line),
           |        Const(JString("column1"))(line)
           |      )(line),
           |      Join(DerefObject, CrossLeftSort,
           |        SplitGroup(1, Vector(LoadIds("/file")))(input)(line),
           |        Const(JString("column2"))(line)
           |      )(line)
           |    )(line)
           |  )(line)
           |""".stripMargin

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
