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

import org.specs2.execute.Result
import org.specs2.mutable.Specification

import com.precog.bytecode.JType.JUnfixedT
import com.precog.yggdrasil.SObject
import com.precog.yggdrasil.test.YId

import scalaz.Failure
import scalaz.Success

trait JoinOptimizerSpecs[M[+_]] extends Specification
    with Evaluator[M]
    with JoinOptimizer
    with StdLib[M]
    with TestConfigComponent[M] 
    with MemoryDatasetConsumer[M] { self =>

  import Function._
  
  import dag._
  import instructions.{ DerefObject, Eq, JoinObject, Line, PushString, WrapObject }

  val testUID = "testUID"

  def testEval(graph: DepGraph)(test: Set[SEvent] => Result): Result = withContext { ctx =>
    (consumeEval(testUID, graph, ctx) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    }) 
  }

  "optimizer" should {
    "eliminate naive cartesian products in trivial cases" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val line = Line(0, "")
      val users = LoadLocal(line,Root(line,PushString("/hom/users")), JUnfixedT)
      val heightWeight = LoadLocal(line,Root(line,PushString("/hom/heightWeight")), JUnfixedT)
      val wheight = Root(line, PushString("height"))
      val dheight = Root(line, PushString("height"))
      val wname = Root(line, PushString("name"))
      val dname = Root(line, PushString("name"))
      val userId = Root(line, PushString("userId"))

      val input =
        Filter(line, IdentitySort,
          Join(line, JoinObject, CrossLeftSort,
              
            Join(line, WrapObject, CrossLeftSort,
              wheight,
              Join(line, DerefObject, CrossLeftSort,
                heightWeight,
                dheight)),
                
            Join(line, WrapObject, CrossLeftSort,
              wname,
              Join(line, DerefObject, CrossLeftSort,
                users,
                dname))),
                
          Join(line, Eq, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              users,
              userId),
            Join(line, DerefObject, CrossLeftSort,
              heightWeight,
              userId)))    
      
      val opt = optimize(input)
              
      val expectedOpt =
        Join(line, JoinObject, IdentitySort,
          Join(line, WrapObject, CrossLeftSort,
            wheight,
            Join(line, DerefObject, CrossLeftSort, 
              SortBy(heightWeight, "userId", "height", 0), 
              Root(line,PushString("height")))),
          Join(line, WrapObject, CrossLeftSort,
            wname,
            Join(line, DerefObject, CrossLeftSort,
              SortBy(users, "userId", "name", 0), 
              Root(line,PushString("name")))))
              
      opt must_== expectedOpt

      /*
      testEval(opt) { result =>
        result.foreach{ _ match {
            case (ids, SObject(obj)) => println(obj)
            case _ =>
        }}

        true
      }
      */
    }
  }
}

object JoinOptimizerSpecs extends JoinOptimizerSpecs[YId] {
  val M = YId.M
  val coM = YId.M
}
