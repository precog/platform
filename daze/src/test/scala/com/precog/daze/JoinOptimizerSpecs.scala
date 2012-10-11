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

  val testUID = "testUID"

  def testEval(graph: DepGraph)(test: Set[SEvent] => Result): Result = withContext { ctx =>
    (consumeEval(testUID, graph, ctx, Path.Root) match {
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
      val users = dag.LoadLocal(line,Root(line,CString("/hom/users")), JUnfixedT)
      val heightWeight = dag.LoadLocal(line,Root(line,CString("/hom/heightWeight")), JUnfixedT)
      val height = Root(line, CString("height"))
      val name = Root(line, CString("name"))
      val userId = Root(line, CString("userId"))
      val key = Root(line, CString("key"))
      val value = Root(line, CString("value"))

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
      val users = dag.LoadLocal(line, Root(line, CString("/users")))
      val heightWeight = dag.LoadLocal(line, Root(line, CString("/heightWeight")))
      val userId = Root(line, CString("userId"))
      val name = Root(line, CString("name"))
      val height = Root(line, CString("height"))
      val weight = Root(line, CString("weight"))
      val key = Root(line, CString("key"))
      val value = Root(line, CString("value"))
      
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
      
      lazy val users = dag.LoadLocal(line, Root(line, CString("/users")))
      lazy val heightWeight = dag.LoadLocal(line, Root(line, CString("/heightWeight")))
      lazy val userId = Root(line, CString("userId"))
      lazy val name = Root(line, CString("name"))
      lazy val key = Root(line, CString("key"))
      lazy val value = Root(line, CString("value"))
      
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
      
      val medals = dag.LoadLocal(line, Root(line, CString("/summer_games/london_medals")))
      val athletes = dag.LoadLocal(line, Root(line, CString("/summer_games/athletes")))
      
      val medalsP = Join(line, JoinObject, IdentitySort,
        medals,
        Join(line, WrapObject, CrossRightSort,
          Root(line, CString("name")),
          Operate(line, BuiltInFunction1Op(toLowerCase),
            Join(line, DerefObject, CrossLeftSort, medals, Root(line, CString("name"))))))
          
      val athletesP = Join(line, JoinObject, IdentitySort,
        athletes,
        Join(line, WrapObject, CrossRightSort,
          Root(line, CString("name")),
          Operate(line, BuiltInFunction1Op(toLowerCase),
            Join(line, DerefObject, CrossLeftSort, athletes, Root(line, CString("name"))))))
            
      val input = Filter(line, IdentitySort,
        Join(line, JoinObject, CrossLeftSort,
          Join(line, WrapObject, CrossRightSort,
            Root(line, CString("winner")),
            Join(line, DerefObject, CrossLeftSort,
              medalsP,
              Root(line, CString("Medal winner")))),
          Join(line, WrapObject, CrossRightSort,
            Root(line, CString("country")),
            Join(line, DerefObject, CrossLeftSort,
              athletesP,
              Root(line, CString("Countryname"))))),
        Join(line, Eq, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort,
            medalsP,
            Root(line, CString("name"))),
          Join(line, DerefObject, CrossLeftSort,
            athletesP,
            Root(line, CString("name")))))
            
      val result = optimizeJoins(input, new IdGen)
      
      val expected =
        Join(line, JoinObject, ValueSort(0),
          Join(line, WrapObject, CrossRightSort,
            Root(line, CString("winner")),
            Join(line, DerefObject, CrossLeftSort,
              SortBy(
                Join(line, JoinObject, IdentitySort,
                  Join(line, WrapObject, CrossLeftSort,
                    Root(line, CString("key")),
                    Join(line, DerefObject, CrossLeftSort,
                      medalsP,
                      Root(line, CString("name")))),
                  Join(line, WrapObject, CrossLeftSort,
                    Root(line, CString("value")),
                    medalsP)),
                "key", "value", 0),
              Root(line, CString("Medal winner")))),
          Join(line, WrapObject, CrossRightSort,
            Root(line, CString("country")),
            Join(line, DerefObject, CrossLeftSort,
              SortBy(
                Join(line, JoinObject, IdentitySort,
                  Join(line, WrapObject, CrossLeftSort,
                    Root(line, CString("key")),
                    Join(line, DerefObject, CrossLeftSort,
                      athletesP,
                      Root(line, CString("name")))),
                  Join(line, WrapObject, CrossLeftSort,
                    Root(line, CString("value")),
                    athletesP)),
                "key", "value", 0),
              Root(line, CString("Countryname")))))
            
      result mustEqual expected
    }
  }
}

object JoinOptimizerSpecs extends JoinOptimizerSpecs[YId] with yggdrasil.test.YIdInstances
