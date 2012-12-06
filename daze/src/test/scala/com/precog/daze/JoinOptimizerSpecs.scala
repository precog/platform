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

import common.Path

import org.specs2.execute.Result
import org.specs2.mutable.Specification

import com.precog.bytecode.JType.JUnfixedT
import com.precog.yggdrasil._
import com.precog.yggdrasil.test.YId
import com.precog.util.IdGen

import scalaz.Failure
import scalaz.Success

trait JoinOptimizerSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with JoinOptimizer
    with PrettyPrinter
    with StdLib[M]
    with MemoryDatasetConsumer[M] { self =>

  import Function._
  
  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph)(test: Set[SEvent] => Result): Result = withContext { ctx =>
    (consumeEval(testAPIKey, graph, ctx, Path.Root) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    }) 
  }

  "join optimization" should {
    "eliminate naive cartesian products in trivial cases" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val line = Line(0, "")
      val users = dag.LoadLocal(line,Const(line,CString("/hom/users")), JUnfixedT)
      val heightWeight = dag.LoadLocal(line,Const(line,CString("/hom/heightWeight")), JUnfixedT)
      val height = Const(line, CString("height"))
      val name = Const(line, CString("name"))
      val userId = Const(line, CString("userId"))
      val key = Const(line, CString("key"))
      val value = Const(line, CString("value"))

      val liftedLHS =
        SortBy(
          Join(line, JoinObject, IdentitySort,
            Join(line, WrapObject, CrossLeftSort,
              key,
              Join(line, DerefObject, CrossLeftSort, heightWeight, userId)),
            Join(line, WrapObject, CrossLeftSort, value, heightWeight)),
          "key", "value", 0)
        
      val liftedRHS =
        SortBy(
          Join(line, JoinObject, IdentitySort,
            Join(line, WrapObject, CrossLeftSort,
              key,
              Join(line, DerefObject, CrossLeftSort, users, userId)),
            Join(line, WrapObject, CrossLeftSort, value, users)),
          "key", "value", 0)
          
      val input =
        Filter(line, IdentitySort,
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              height,
              Join(line, DerefObject, CrossLeftSort, heightWeight, height)),
            Join(line, WrapObject, CrossLeftSort,
              name,
              Join(line, DerefObject, CrossLeftSort, users, name))),
          Join(line, Eq, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              users,
              userId),
            Join(line, DerefObject, CrossLeftSort,
              heightWeight,
              userId)))    
      
      val opt = optimizeJoins(input, new IdGen)
      
      val expectedOpt =
        Join(line, JoinObject, ValueSort(0),
          Join(line, WrapObject, CrossLeftSort,
            height,
            Join(line, DerefObject, CrossLeftSort, liftedLHS, height)),
          Join(line, WrapObject, CrossLeftSort,
            name,
            Join(line, DerefObject, CrossLeftSort, liftedRHS, name)))

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in slightly less trivial cases (1)" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height, weight: b.weight } where a.userId = b.userId """.stripMargin
        
      val line = Line(0, "")
      val users = dag.LoadLocal(line, Const(line, CString("/users")))
      val heightWeight = dag.LoadLocal(line, Const(line, CString("/heightWeight")))
      val userId = Const(line, CString("userId"))
      val name = Const(line, CString("name"))
      val height = Const(line, CString("height"))
      val weight = Const(line, CString("weight"))
      val key = Const(line, CString("key"))
      val value = Const(line, CString("value"))
      
      val liftedLHS =
        SortBy(
          Join(line, JoinObject, IdentitySort,
            Join(line, WrapObject, CrossLeftSort,
              key,
              Join(line, DerefObject, CrossLeftSort, heightWeight, userId)),
            Join(line, WrapObject, CrossLeftSort, value, heightWeight)),
          "key", "value", 0)
        
      val liftedRHS =
        SortBy(
          Join(line, JoinObject, IdentitySort,
            Join(line, WrapObject, CrossLeftSort,
              key,
              Join(line, DerefObject, CrossLeftSort, users, userId)),
            Join(line, WrapObject, CrossLeftSort, value, users)),
          "key", "value", 0)
          
      val input =
        Filter(line, IdentitySort,
          Join(line, JoinObject, CrossLeftSort,
            Join(line, JoinObject, IdentitySort,
              Join(line, WrapObject, CrossLeftSort,
                height,
                Join(line, DerefObject, CrossLeftSort, heightWeight, height)
              ),
              Join(line, WrapObject, CrossLeftSort,
                weight,
                Join(line, DerefObject, CrossLeftSort, heightWeight, weight)
              )
            ),
            Join(line, WrapObject, CrossLeftSort,
              name,
              Join(line, DerefObject, CrossLeftSort, users, name)
            )
          ),
          Join(line, Eq, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort, users, userId),
            Join(line, DerefObject, CrossLeftSort, heightWeight, userId)
          )
        )

      val opt = optimizeJoins(input, new IdGen)
      
      val expectedOpt =
        Join(line, JoinObject, ValueSort(0),
          Join(line, JoinObject, ValueSort(0),
            Join(line, WrapObject, CrossLeftSort,
              height,
              Join(line, DerefObject, CrossLeftSort, liftedLHS, height)),
            Join(line, WrapObject, CrossLeftSort,
              weight,
              Join(line, DerefObject, CrossLeftSort, liftedLHS, weight))),
          Join(line, WrapObject, CrossLeftSort,
            name,
            Join(line, DerefObject, CrossLeftSort, liftedRHS, name)))

       opt must_== expectedOpt
    }

    "eliminate naive cartesian products in slightly less trivial cases (2)" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   ({ name: a.name } with b) where a.userId = b.userId """.stripMargin

      val line = Line(0, "")
      
      lazy val users = dag.LoadLocal(line, Const(line, CString("/users")))
      lazy val heightWeight = dag.LoadLocal(line, Const(line, CString("/heightWeight")))
      lazy val userId = Const(line, CString("userId"))
      lazy val name = Const(line, CString("name"))
      lazy val key = Const(line, CString("key"))
      lazy val value = Const(line, CString("value"))
      
      lazy val input =
        Filter(line, IdentitySort,
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              name,
              Join(line, DerefObject, CrossLeftSort, users, name)
            ),
            heightWeight
          ),
          Join(line, Eq, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort, users, userId),
            Join(line, DerefObject, CrossLeftSort, heightWeight, userId)
          )
        )

      val opt = optimizeJoins(input, new IdGen)

      val expectedOpt =
        Join(line, JoinObject, ValueSort(0),
          Join(line, WrapObject, CrossLeftSort,
            name,
            Join(line, DerefObject, CrossLeftSort,
              SortBy(
                Join(line, JoinObject, IdentitySort,
                  Join(line, WrapObject, CrossLeftSort,
                    key,
                    Join(line, DerefObject, CrossLeftSort, users, userId)),
                  Join(line, WrapObject, CrossLeftSort, value, users)),
                "key", "value", 0),
              name)),
          SortBy(
            Join(line, JoinObject, IdentitySort,
              Join(line, WrapObject, CrossLeftSort,
                key,
                Join(line, DerefObject, CrossLeftSort, heightWeight, userId)),
              Join(line, WrapObject, CrossLeftSort, value, heightWeight)),
            "key", "value", 0))

      opt must_== expectedOpt
    }
    
    "eliminate cartesian in medal winners query" in {
      /*
       * import std::string::toLowerCase
       *
       * medals := //summer_games/london_medals
       * athletes := //summer_games/athletes
       *
       * medals' := medals with { name: toLowerCase(medals.name) }
       * athletes' := athletes with { name: toLowerCase(athletes.name) }
       * 
       * medals' ~ athletes'
       *   { winner: medals'.`Medal winner`, country: athletes'.Countryname }
       *     where medals'.name = athletes'.name
       */
       
      val line = Line(0, "")
      
      val medals = dag.LoadLocal(line, Const(line, CString("/summer_games/london_medals")))
      val athletes = dag.LoadLocal(line, Const(line, CString("/summer_games/athletes")))
      
      val medalsP = Join(line, JoinObject, IdentitySort,
        medals,
        Join(line, WrapObject, CrossRightSort,
          Const(line, CString("name")),
          Operate(line, BuiltInFunction1Op(toLowerCase),
            Join(line, DerefObject, CrossLeftSort, medals, Const(line, CString("name"))))))
          
      val athletesP = Join(line, JoinObject, IdentitySort,
        athletes,
        Join(line, WrapObject, CrossRightSort,
          Const(line, CString("name")),
          Operate(line, BuiltInFunction1Op(toLowerCase),
            Join(line, DerefObject, CrossLeftSort, athletes, Const(line, CString("name"))))))
            
      val input = Filter(line, IdentitySort,
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossRightSort,
            Const(line, CString("winner")),
            Join(line, DerefObject, CrossLeftSort,
              medalsP,
              Const(line, CString("Medal winner")))),
          Join(line, WrapObject, CrossRightSort,
            Const(line, CString("country")),
            Join(line, DerefObject, CrossLeftSort,
              athletesP,
              Const(line, CString("Countryname"))))),
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            medalsP,
            Const(line, CString("name"))),
          Join(line, DerefObject, CrossLeftSort,
            athletesP,
            Const(line, CString("name")))))
            
      val result = optimizeJoins(input, new IdGen)
      
      val expected =
        Join(line, JoinObject, ValueSort(0),
          Join(line, WrapObject, CrossRightSort,
            Const(line, CString("winner")),
            Join(line, DerefObject, CrossLeftSort,
              SortBy(
                Join(line, JoinObject, IdentitySort,
                  Join(line, WrapObject, CrossLeftSort,
                    Const(line, CString("key")),
                    Join(line, DerefObject, CrossLeftSort,
                      medalsP,
                      Const(line, CString("name")))),
                  Join(line, WrapObject, CrossLeftSort,
                    Const(line, CString("value")),
                    medalsP)),
                "key", "value", 0),
              Const(line, CString("Medal winner")))),
          Join(line, WrapObject, CrossRightSort,
            Const(line, CString("country")),
            Join(line, DerefObject, CrossLeftSort,
              SortBy(
                Join(line, JoinObject, IdentitySort,
                  Join(line, WrapObject, CrossLeftSort,
                    Const(line, CString("key")),
                    Join(line, DerefObject, CrossLeftSort,
                      athletesP,
                      Const(line, CString("name")))),
                  Join(line, WrapObject, CrossLeftSort,
                    Const(line, CString("value")),
                    athletesP)),
                "key", "value", 0),
              Const(line, CString("Countryname")))))
            
      result mustEqual expected
    }
  }
}

object JoinOptimizerSpecs extends JoinOptimizerSpecs[YId] with yggdrasil.test.YIdInstances
