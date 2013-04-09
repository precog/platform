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

import com.precog.common._
import common.Path

import org.specs2.execute.Result
import org.specs2.mutable.Specification

import com.precog.bytecode.JType.JUniverseT
import com.precog.yggdrasil._
import com.precog.yggdrasil.test.YId
import com.precog.util.IdGen

import scala.Function._

import scalaz.Failure
import scalaz.Success

trait JoinOptimizerSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with JoinOptimizer
    with LongIdMemoryDatasetConsumer[M] { self =>
  
  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph)(test: Set[SEvent] => Result): Result = {
    (consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    }) 
  }

  "join optimization" should {
    "eliminate naive cartesian products in trivial object join cases" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val line = Line(1, 1, "")
      val users = dag.LoadLocal(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.LoadLocal(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        SortBy(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, CrossLeftSort,
              key,
              Join(DerefObject, CrossLeftSort, heightWeight, userId)(line))(line),
            Join(WrapObject, CrossLeftSort, value, heightWeight)(line))(line),
          "key", "value", 0)
        
      val liftedRHS =
        SortBy(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, CrossLeftSort,
              key,
              Join(DerefObject, CrossLeftSort, users, userId)(line))(line),
            Join(WrapObject, CrossLeftSort, value, users)(line))(line),
          "key", "value", 0)
          
      val input =
        Filter(IdentitySort,
          Join(JoinObject, CrossLeftSort,
            Join(WrapObject, CrossLeftSort,
              height,
              Join(DerefObject, CrossLeftSort, heightWeight, height)(line))(line),
            Join(WrapObject, CrossLeftSort,
              name,
              Join(DerefObject, CrossLeftSort, users, name)(line))(line))(line),
          Join(Eq, CrossLeftSort,
            Join(DerefObject, CrossLeftSort,
              users,
              userId)(line),
            Join(DerefObject, CrossLeftSort,
              heightWeight,
              userId)(line))(line))(line)    
      
      val opt = optimizeJoins(input, new IdGen)
      
      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, CrossLeftSort,
            height,
            Join(DerefObject, CrossLeftSort, liftedLHS, height)(line))(line),
          Join(WrapObject, CrossLeftSort,
            name,
            Join(DerefObject, CrossLeftSort, liftedRHS, name)(line))(line))(line)

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in trivial array join cases" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   [b.height, a.name] where a.userId = b.userId """.stripMargin
        
      val line = Line(1, 1, "")
      val users = dag.LoadLocal(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.LoadLocal(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        SortBy(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, CrossLeftSort,
              key,
              Join(DerefObject, CrossLeftSort, heightWeight, userId)(line))(line),
            Join(WrapObject, CrossLeftSort, value, heightWeight)(line))(line),
          "key", "value", 0)
        
      val liftedRHS =
        SortBy(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, CrossLeftSort,
              key,
              Join(DerefObject, CrossLeftSort, users, userId)(line))(line),
            Join(WrapObject, CrossLeftSort, value, users)(line))(line),
          "key", "value", 0)
          
      val input =
        Filter(IdentitySort,
          Join(JoinArray, CrossLeftSort,
            Operate(WrapArray,
              Join(DerefObject, CrossLeftSort, heightWeight, height)(line))(line),
            Operate(WrapArray,
              Join(DerefObject, CrossLeftSort, users, name)(line))(line))(line),
          Join(Eq, CrossLeftSort,
            Join(DerefObject, CrossLeftSort,
              users,
              userId)(line),
            Join(DerefObject, CrossLeftSort,
              heightWeight,
              userId)(line))(line))(line)    
      
      val opt = optimizeJoins(input, new IdGen)
      
      val expectedOpt =
        Join(JoinArray, ValueSort(0),
          Operate(WrapArray,
            Join(DerefObject, CrossLeftSort, liftedLHS, height)(line))(line),
          Operate(WrapArray,
            Join(DerefObject, CrossLeftSort, liftedRHS, name)(line))(line))(line)

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in slightly less trivial cases (1)" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height, weight: b.weight } where a.userId = b.userId """.stripMargin
        
      val line = Line(1, 1, "")
      val users = dag.LoadLocal(Const(CString("/users"))(line))(line)
      val heightWeight = dag.LoadLocal(Const(CString("/heightWeight"))(line))(line)
      val userId = Const(CString("userId"))(line)
      val name = Const(CString("name"))(line)
      val height = Const(CString("height"))(line)
      val weight = Const(CString("weight"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)
      
      val liftedLHS =
        SortBy(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, CrossLeftSort,
              key,
              Join(DerefObject, CrossLeftSort, heightWeight, userId)(line))(line),
            Join(WrapObject, CrossLeftSort, value, heightWeight)(line))(line),
          "key", "value", 0)
        
      val liftedRHS =
        SortBy(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, CrossLeftSort,
              key,
              Join(DerefObject, CrossLeftSort, users, userId)(line))(line),
            Join(WrapObject, CrossLeftSort, value, users)(line))(line),
          "key", "value", 0)
          
      val input =
        Filter(IdentitySort,
          Join(JoinObject, CrossLeftSort,
            Join(JoinObject, IdentitySort,
              Join(WrapObject, CrossLeftSort,
                height,
                Join(DerefObject, CrossLeftSort, heightWeight, height)(line)
              )(line),
              Join(WrapObject, CrossLeftSort,
                weight,
                Join(DerefObject, CrossLeftSort, heightWeight, weight)(line)
              )(line)
            )(line),
            Join(WrapObject, CrossLeftSort,
              name,
              Join(DerefObject, CrossLeftSort, users, name)(line)
            )(line)
          )(line),
          Join(Eq, CrossLeftSort,
            Join(DerefObject, CrossLeftSort, users, userId)(line),
            Join(DerefObject, CrossLeftSort, heightWeight, userId)(line)
          )(line)
        )(line)

      val opt = optimizeJoins(input, new IdGen)
      
      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(JoinObject, ValueSort(0),
            Join(WrapObject, CrossLeftSort,
              height,
              Join(DerefObject, CrossLeftSort, liftedLHS, height)(line))(line),
            Join(WrapObject, CrossLeftSort,
              weight,
              Join(DerefObject, CrossLeftSort, liftedLHS, weight)(line))(line))(line),
          Join(WrapObject, CrossLeftSort,
            name,
            Join(DerefObject, CrossLeftSort, liftedRHS, name)(line))(line))(line)

       opt must_== expectedOpt
    }

    "eliminate naive cartesian products in slightly less trivial cases (2)" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   ({ name: a.name } with b) where a.userId = b.userId """.stripMargin

      val line = Line(1, 1, "")
      
      lazy val users = dag.LoadLocal(Const(CString("/users"))(line))(line)
      lazy val heightWeight = dag.LoadLocal(Const(CString("/heightWeight"))(line))(line)
      lazy val userId = Const(CString("userId"))(line)
      lazy val name = Const(CString("name"))(line)
      lazy val key = Const(CString("key"))(line)
      lazy val value = Const(CString("value"))(line)
      
      lazy val input =
        Filter(IdentitySort,
          Join(JoinObject, CrossLeftSort,
            Join(WrapObject, CrossLeftSort,
              name,
              Join(DerefObject, CrossLeftSort, users, name)(line)
            )(line),
            heightWeight
          )(line),
          Join(Eq, CrossLeftSort,
            Join(DerefObject, CrossLeftSort, users, userId)(line),
            Join(DerefObject, CrossLeftSort, heightWeight, userId)(line)
          )(line)
        )(line)

      val opt = optimizeJoins(input, new IdGen)

      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, CrossLeftSort,
            name,
            Join(DerefObject, CrossLeftSort,
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    key,
                    Join(DerefObject, CrossLeftSort, users, userId)(line))(line),
                  Join(WrapObject, CrossLeftSort, value, users)(line))(line),
                "key", "value", 0),
              name)(line))(line),
          SortBy(
            Join(JoinObject, IdentitySort,
              Join(WrapObject, CrossLeftSort,
                key,
                Join(DerefObject, CrossLeftSort, heightWeight, userId)(line))(line),
              Join(WrapObject, CrossLeftSort, value, heightWeight)(line))(line),
            "key", "value", 0))(line)

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
       
      val line = Line(1, 1, "")
      
      val medals = dag.LoadLocal(Const(CString("/summer_games/london_medals"))(line))(line)
      val athletes = dag.LoadLocal(Const(CString("/summer_games/athletes"))(line))(line)
      
      val medalsP = Join(JoinObject, IdentitySort,
        medals,
        Join(WrapObject, CrossRightSort,
          Const(CString("name"))(line),
          Operate(BuiltInFunction1Op(toLowerCase),
            Join(DerefObject, CrossLeftSort, medals, Const(CString("name"))(line))(line))(line))(line))(line)
          
      val athletesP = Join(JoinObject, IdentitySort,
        athletes,
        Join(WrapObject, CrossRightSort,
          Const(CString("name"))(line),
          Operate(BuiltInFunction1Op(toLowerCase),
            Join(DerefObject, CrossLeftSort, athletes, Const(CString("name"))(line))(line))(line))(line))(line)
            
      val input = Filter(IdentitySort,
        Join(JoinObject, CrossLeftSort,
          Join(WrapObject, CrossRightSort,
            Const(CString("winner"))(line),
            Join(DerefObject, CrossLeftSort,
              medalsP,
              Const(CString("Medal winner"))(line))(line))(line),
          Join(WrapObject, CrossRightSort,
            Const(CString("country"))(line),
            Join(DerefObject, CrossLeftSort,
              athletesP,
              Const(CString("Countryname"))(line))(line))(line))(line),
        Join(Eq, CrossLeftSort,
          Join(DerefObject, CrossLeftSort,
            medalsP,
            Const(CString("name"))(line))(line),
          Join(DerefObject, CrossLeftSort,
            athletesP,
            Const(CString("name"))(line))(line))(line))(line)
            
      val result = optimizeJoins(input, new IdGen)
      
      val expected =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, CrossRightSort,
            Const(CString("winner"))(line),
            Join(DerefObject, CrossLeftSort,
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("key"))(line),
                    Join(DerefObject, CrossLeftSort,
                      medalsP,
                      Const(CString("name"))(line))(line))(line),
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("value"))(line),
                    medalsP)(line))(line),
                "key", "value", 0),
              Const(CString("Medal winner"))(line))(line))(line),
          Join(WrapObject, CrossRightSort,
            Const(CString("country"))(line),
            Join(DerefObject, CrossLeftSort,
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("key"))(line),
                    Join(DerefObject, CrossLeftSort,
                      athletesP,
                      Const(CString("name"))(line))(line))(line),
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("value"))(line),
                    athletesP)(line))(line),
                "key", "value", 0),
              Const(CString("Countryname"))(line))(line))(line))(line)
            
      result mustEqual expected
    }
    
    "produce a valid dag for a ternary object-literal cartesian" in {
      /*
       * clicks := //clicks
       * 
       * clicks' := new clicks
       * clicks ~ clicks'
       *   { a: clicks, b: clicks', c: clicks } where clicks'.pageId = clicks.pageId
       */
      
      val line = Line(1, 1, "")
      
      lazy val clicks = dag.LoadLocal(Const(CString("/clicks"))(line))(line)
      lazy val clicksP = dag.New(clicks)(line)
      
      lazy val input =
        Filter(IdentitySort,
          Join(JoinObject, CrossLeftSort,
            Join(JoinObject, IdentitySort,
              Join(WrapObject, CrossLeftSort,
                Const(CString("a"))(line),
                clicks
              )(line),
              Join(WrapObject, CrossLeftSort,
                Const(CString("c"))(line),
                clicks
              )(line)
            )(line),
            Join(WrapObject, CrossLeftSort,
              Const(CString("b"))(line),
              clicksP
            )(line)
          )(line),
          Join(Eq, CrossLeftSort,
            Join(DerefObject, CrossLeftSort,
              clicks,
              Const(CString("pageId"))(line)
            )(line),
            Join(DerefObject, CrossLeftSort,
              clicksP,
              Const(CString("pageId"))(line)
            )(line)
          )(line)
        )(line)

        lazy val clickPages =
          SortBy(
            Join(JoinObject, IdentitySort,
              Join(WrapObject, CrossLeftSort,
                Const(CString("key"))(line),
                Join(DerefObject, CrossLeftSort,
                  clicks,
                  Const(CString("pageId"))(line)
                )(line)
              )(line),
              Join(WrapObject, CrossLeftSort,
                Const(CString("value"))(line),
                clicks
              )(line)
            )(line),
            "key", "value", 0
          )
        
        lazy val expected =
          Join(JoinObject, ValueSort(0),
            Join(JoinObject, ValueSort(0),
              Join(WrapObject, CrossLeftSort,
                Const(CString("a"))(line),
                clickPages
              )(line),
              Join(WrapObject, CrossLeftSort,
                Const(CString("c"))(line),
                clickPages
              )(line)
            )(line),
            Join(WrapObject, CrossLeftSort,
              Const(CString("b"))(line),
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("key"))(line),
                    Join(DerefObject, CrossLeftSort,
                      clicksP,
                      Const(CString("pageId"))(line)
                    )(line)
                  )(line),
                  Join(WrapObject, CrossLeftSort,
                    Const(CString("value"))(line),
                    clicksP
                  )(line)
                )(line),
                "key", "value", 0
              )
            )(line)
          )(line)
        
      optimizeJoins(input, new IdGen) mustEqual expected
    }
  }
}

object JoinOptimizerSpecs extends JoinOptimizerSpecs[YId] with yggdrasil.test.YIdInstances 
