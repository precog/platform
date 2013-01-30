package com.precog
package daze

import common.Path

import org.specs2.execute.Result
import org.specs2.mutable.Specification

import com.precog.bytecode.JType.JUnfixedT
import com.precog.yggdrasil._
import com.precog.yggdrasil.test.YId
import com.precog.util.IdGen

import blueeyes.json._

import scala.Function._

import scalaz.Failure
import scalaz.Success

trait JoinOptimizerSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with JoinOptimizer
    with PrettyPrinter
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
    "eliminate naive cartesian products in trivial cases" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height } where a.userId = b.userId """.stripMargin
        
      val line = Line(1, 1, "")
      val users = dag.LoadLocal(Const(JString("/hom/users"))(line), JUnfixedT)(line)
      val heightWeight = dag.LoadLocal(Const(JString("/hom/heightWeight"))(line), JUnfixedT)(line)
      val height = Const(JString("height"))(line)
      val name = Const(JString("name"))(line)
      val userId = Const(JString("userId"))(line)
      val key = Const(JString("key"))(line)
      val value = Const(JString("value"))(line)

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
      
      val opt = optimizeJoins(input, Set.empty, new IdGen)
      
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

    "eliminate naive cartesian products in slightly less trivial cases (1)" in {
      
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height, weight: b.weight } where a.userId = b.userId """.stripMargin
        
      val line = Line(1, 1, "")
      val users = dag.LoadLocal(Const(JString("/users"))(line))(line)
      val heightWeight = dag.LoadLocal(Const(JString("/heightWeight"))(line))(line)
      val userId = Const(JString("userId"))(line)
      val name = Const(JString("name"))(line)
      val height = Const(JString("height"))(line)
      val weight = Const(JString("weight"))(line)
      val key = Const(JString("key"))(line)
      val value = Const(JString("value"))(line)
      
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

      val opt = optimizeJoins(input, Set.empty, new IdGen)
      
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
      
      lazy val users = dag.LoadLocal(Const(JString("/users"))(line))(line)
      lazy val heightWeight = dag.LoadLocal(Const(JString("/heightWeight"))(line))(line)
      lazy val userId = Const(JString("userId"))(line)
      lazy val name = Const(JString("name"))(line)
      lazy val key = Const(JString("key"))(line)
      lazy val value = Const(JString("value"))(line)
      
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

      val opt = optimizeJoins(input, Set.empty, new IdGen)

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
      
      val medals = dag.LoadLocal(Const(JString("/summer_games/london_medals"))(line))(line)
      val athletes = dag.LoadLocal(Const(JString("/summer_games/athletes"))(line))(line)
      
      val medalsP = Join(JoinObject, IdentitySort,
        medals,
        Join(WrapObject, CrossRightSort,
          Const(JString("name"))(line),
          Operate(BuiltInFunction1Op(toLowerCase),
            Join(DerefObject, CrossLeftSort, medals, Const(JString("name"))(line))(line))(line))(line))(line)
          
      val athletesP = Join(JoinObject, IdentitySort,
        athletes,
        Join(WrapObject, CrossRightSort,
          Const(JString("name"))(line),
          Operate(BuiltInFunction1Op(toLowerCase),
            Join(DerefObject, CrossLeftSort, athletes, Const(JString("name"))(line))(line))(line))(line))(line)
            
      val input = Filter(IdentitySort,
        Join(JoinObject, CrossLeftSort,
          Join(WrapObject, CrossRightSort,
            Const(JString("winner"))(line),
            Join(DerefObject, CrossLeftSort,
              medalsP,
              Const(JString("Medal winner"))(line))(line))(line),
          Join(WrapObject, CrossRightSort,
            Const(JString("country"))(line),
            Join(DerefObject, CrossLeftSort,
              athletesP,
              Const(JString("Countryname"))(line))(line))(line))(line),
        Join(Eq, CrossLeftSort,
          Join(DerefObject, CrossLeftSort,
            medalsP,
            Const(JString("name"))(line))(line),
          Join(DerefObject, CrossLeftSort,
            athletesP,
            Const(JString("name"))(line))(line))(line))(line)
            
      val result = optimizeJoins(input, Set.empty, new IdGen)
      
      val expected =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, CrossRightSort,
            Const(JString("winner"))(line),
            Join(DerefObject, CrossLeftSort,
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(JString("key"))(line),
                    Join(DerefObject, CrossLeftSort,
                      medalsP,
                      Const(JString("name"))(line))(line))(line),
                  Join(WrapObject, CrossLeftSort,
                    Const(JString("value"))(line),
                    medalsP)(line))(line),
                "key", "value", 0),
              Const(JString("Medal winner"))(line))(line))(line),
          Join(WrapObject, CrossRightSort,
            Const(JString("country"))(line),
            Join(DerefObject, CrossLeftSort,
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(JString("key"))(line),
                    Join(DerefObject, CrossLeftSort,
                      athletesP,
                      Const(JString("name"))(line))(line))(line),
                  Join(WrapObject, CrossLeftSort,
                    Const(JString("value"))(line),
                    athletesP)(line))(line),
                "key", "value", 0),
              Const(JString("Countryname"))(line))(line))(line))(line)
            
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
      
      lazy val clicks = dag.LoadLocal(Const(JString("/clicks"))(line))(line)
      lazy val clicksP = dag.New(clicks)(line)
      
      lazy val input =
        Filter(IdentitySort,
          Join(JoinObject, CrossLeftSort,
            Join(JoinObject, IdentitySort,
              Join(WrapObject, CrossLeftSort,
                Const(JString("a"))(line),
                clicks
              )(line),
              Join(WrapObject, CrossLeftSort,
                Const(JString("c"))(line),
                clicks
              )(line)
            )(line),
            Join(WrapObject, CrossLeftSort,
              Const(JString("b"))(line),
              clicksP
            )(line)
          )(line),
          Join(Eq, CrossLeftSort,
            Join(DerefObject, CrossLeftSort,
              clicks,
              Const(JString("pageId"))(line)
            )(line),
            Join(DerefObject, CrossLeftSort,
              clicksP,
              Const(JString("pageId"))(line)
            )(line)
          )(line)
        )(line)

        lazy val clickPages =
          SortBy(
            Join(JoinObject, IdentitySort,
              Join(WrapObject, CrossLeftSort,
                Const(JString("key"))(line),
                Join(DerefObject, CrossLeftSort,
                  clicks,
                  Const(JString("pageId"))(line)
                )(line)
              )(line),
              Join(WrapObject, CrossLeftSort,
                Const(JString("value"))(line),
                clicks
              )(line)
            )(line),
            "key", "value", 0
          )
        
        lazy val expected =
          Join(JoinObject, ValueSort(0),
            Join(JoinObject, ValueSort(0),
              Join(WrapObject, CrossLeftSort,
                Const(JString("a"))(line),
                clickPages
              )(line),
              Join(WrapObject, CrossLeftSort,
                Const(JString("c"))(line),
                clickPages
              )(line)
            )(line),
            Join(WrapObject, CrossLeftSort,
              Const(JString("b"))(line),
              SortBy(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, CrossLeftSort,
                    Const(JString("key"))(line),
                    Join(DerefObject, CrossLeftSort,
                      clicksP,
                      Const(JString("pageId"))(line)
                    )(line)
                  )(line),
                  Join(WrapObject, CrossLeftSort,
                    Const(JString("value"))(line),
                    clicksP
                  )(line)
                )(line),
                "key", "value", 0
              )
            )(line)
          )(line)
        
      optimizeJoins(input, Set.empty, new IdGen) mustEqual expected
    }
  }
}

object JoinOptimizerSpecs extends JoinOptimizerSpecs[YId] with yggdrasil.test.YIdInstances 
