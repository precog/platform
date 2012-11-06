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
package com.precog.yggdrasil
package table

import com.precog.common.Path
import com.precog.common.json._
import com.precog.common.VectorCase
import com.precog.bytecode.JType
import com.precog.yggdrasil.util._

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import com.weiglewilczek.slf4s.Logging

import scala.annotation.tailrec
import scala.collection.mutable.LinkedHashSet
import scala.util.Random

import scalaz._
import scalaz.effect.IO 
import scalaz.std.anyVal._
import scalaz.std.stream._
import scalaz.syntax.copointed._
import scalaz.syntax.show._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait GroupingSupportSpec[M[+_]] extends BlockStoreTestSupport[M] with Specification with ScalaCheck {
  import TableModule._

  //def constraint(str: String) = OrderingConstraint(str.split(",").toSeq.map(_.toSet.map((c: Char) => CPathField(c.toString))))
  abstract class RunVictimPairTest[M[+_]](val module: ColumnarTableModuleTestSupport[M]) {
    import module._
    import trans._
    import trans.constants._

    import Table._
    import Table.Universe._
    import OrderingConstraints._
    import GroupKeyTrans._

    type GroupId = module.GroupId

    def ticvars(str: String): Seq[TicVar] = str.toSeq.map((c: Char) => CPathField(c.toString))
    def order(str: String) = OrderingConstraint.fromFixed(ticvars(str))
    def mergeNode(str: String) = MergeNode(ticvars(str).toSet, null)

    def generate(v1GroupId: GroupId, v2GroupId: GroupId)(implicit show: Show[GroupId]): (List[JValue], List[JValue], List[JValue], Boolean, Boolean)

    def run = {
      val v1GroupId = newGroupId
      val v2GroupId = newGroupId

      val (victim1Data, victim2Data, expected, v1ByIdentity, v2ByIdentity) = generate(v1GroupId, v2GroupId)

      val victim1Source = MergeNode(
        Binding(fromJson(victim1Data.toStream),
                SourceKey.Single,
                Some(TransSpec1.Id),
                v1GroupId,
                GroupKeySpecAnd(
                  GroupKeySpecSource(CPathField("a"), DerefObjectStatic(Leaf(Source), CPathField("a0"))),
                  GroupKeySpecSource(CPathField("b"), DerefObjectStatic(Leaf(Source), CPathField("b0")))))
      )

      val victim1 = BorgVictimNode(
        NodeSubset(
          victim1Source, 
          victim1Source.binding.source, 
          SourceKey.Single, 
          Some(TransSpec1.Id),
          GroupKeyTrans(
            OuterObjectConcat(
              WrapObject(DerefObjectStatic(SourceValue.Single, CPathField("a0")), "000000"),
              WrapObject(DerefObjectStatic(SourceValue.Single, CPathField("b0")), "000001")
            ),
            ticvars("ab")),
          ticvars("ab"),
          v1ByIdentity
        )
      )

      val victim2Source = MergeNode(
        Binding(fromJson(victim2Data.toStream),
                SourceKey.Single,
                Some(TransSpec1.Id),
                v2GroupId,
                GroupKeySpecSource(CPathField("a"), DerefObjectStatic(Leaf(Source), CPathField("a0"))))
      )

      val victim2 = BorgVictimNode(
        NodeSubset(
          victim2Source, 
          victim2Source.binding.source, 
          SourceKey.Single, 
          Some(TransSpec1.Id),
          GroupKeyTrans(
            OuterObjectConcat(
              WrapObject(DerefObjectStatic(SourceValue.Single, CPathField("a0")), "000000")
            ),
            ticvars("a")),
          ticvars("a"),
          v2ByIdentity
        )
      )

      val requiredOrders = Map(
        victim1Source -> Set(ticvars("a")),
        victim2Source -> Set(ticvars("a"))
      )

      val BorgResultNode(BorgResult(table, keys, groups, size, _)) = Table.join(victim1, victim2, requiredOrders).copoint

      toJson(table).copoint must_== expected.toStream
    }
  }

  def testJoinOnPreSortedVictims[M[+_]](module: ColumnarTableModuleTestSupport[M]) = {
    val test = new RunVictimPairTest[M](module) {
      def generate(v1GroupId: GroupId, v2GroupId: GroupId)(implicit show: Show[GroupId]) = {
        val JArray(victim1Data) = JsonParser.parse("""[
          {"key": [1], "value": {"a0": 3, "b0": 7}},
          {"key": [2], "value": {"a0": 5, "b0": 11}},
        ]""")

        val JArray(victim2Data) = JsonParser.parse("""[
          {"key": [3], "value": {"a0": 3, "c": 17}},
          {"key": [4], "value": {"a0": 13, "c": 19}},
        ]""")

        val JArray(expected) = JsonParser.parse("""[
          {
            "values":{
              "%1$s":{
                "value":{
                  "b0":7,
                  "a0":3
                },
                "key":[1]
              },
              "%2$s":{
                "value":{
                  "c":17,
                  "a0":3
                },
                "key":[3]
              }
            },
            "identities":{
              "%2$s":[3],
              "%1$s":[1]
            },
            "groupKeys":{
              "000000":3,
              "000001":7
            }
          }
        ]""".format(v1GroupId.shows, v2GroupId.shows))

        (victim1Data, victim2Data, expected, false, false) 
      }
    }

    test.run
  }

  def testJoinOnPartiallySortedVictims[M[+_]](module: ColumnarTableModuleTestSupport[M]) = {
    val test = new RunVictimPairTest[M](module) {
      def generate(v1GroupId: GroupId, v2GroupId: GroupId)(implicit show: Show[GroupId]) = {

        val JArray(victim1Data) = JsonParser.parse("""[
          {"key": [1], "value": {"a0": 5, "b0": 7}},
          {"key": [2], "value": {"a0": 3, "b0": 11}},
        ]""")

        val JArray(victim2Data) = JsonParser.parse("""[
          {"key": [3], "value": {"a0": 3, "c": 17}},
          {"key": [4], "value": {"a0": 13, "c": 19}},
        ]""")

        val JArray(expected) = JsonParser.parse("""[
          {
            "values":{
              "%1$s":{
                "value":{
                  "b0":11,
                  "a0":3
                },
                "key":[2]
              },
              "%2$s":{
                "value":{
                  "c":17,
                  "a0":3
                },
                "key":[3]
              }
            },
            "identities":{
              "%2$s":[3],
              "%1$s":[2]
            },
            "groupKeys":{
              "000000":3,
              "000001":11
            }
          }
        ]""".format(v1GroupId.shows, v2GroupId.shows))

        (victim1Data, victim2Data, expected, true, false)
      }
    }

    test.run
  }

  def testJoinOnUnsortedVictims[M[+_]](module: ColumnarTableModuleTestSupport[M]) = {
    val test = new RunVictimPairTest[M](module) {
      def generate(v1GroupId: GroupId, v2GroupId: GroupId)(implicit show: Show[GroupId]) = {

        val JArray(victim1Data) = JsonParser.parse("""[
          {"key": [1], "value": {"a0": 5, "b0": 7}},
          {"key": [2], "value": {"a0": 3, "b0": 11}},
        ]""")

        val JArray(victim2Data) = JsonParser.parse("""[
          {"key": [3], "value": {"a0": 13, "c": 17}},
          {"key": [4], "value": {"a0": 3, "c": 19}},
        ]""")

        val JArray(expected) = JsonParser.parse("""[
          {
            "values":{
              "%1$s":{
                "value":{
                  "b0":11,
                  "a0":3
                },
                "key":[2]
              },
              "%2$s":{
                "value":{
                  "c":19,
                  "a0":3
                },
                "key":[4]
              }
            },
            "identities":{
              "%2$s":[4],
              "%1$s":[2]
            },
            "groupKeys":{
              "000000":3,
              "000001":11
            }
          }
        ]""".format(v1GroupId.shows, v2GroupId.shows))

        (victim1Data, victim2Data, expected, true, true)
      }
    }

    test.run
  }

  "join" should {
    "combine a pair of already-group-sorted victims" in testJoinOnPreSortedVictims(emptyTestModule)
    "combine a pair where one victim is sorted by identity" in testJoinOnPartiallySortedVictims(emptyTestModule)
    "combine a pair where both victims are sorted by identity" in testJoinOnUnsortedVictims(emptyTestModule)
  }

  /*
  import OrderingConstraint2._

  def c(str: String) = OrderingConstraint2.parse(str)

  object ConstraintParser extends scala.util.parsing.combinator.JavaTokenParsers {
    lazy val ticVar: Parser[OrderingConstraint2] = "'" ~> ident ^^ (s => Variable(CPathField(s)))

    lazy val ordered: Parser[OrderingConstraint2] = ("[" ~> repsep(constraint, ",") <~ "]") ^^ (v => Ordered(v.toSeq))

    lazy val unordered: Parser[OrderingConstraint2] = ("{" ~> repsep(constraint, ",") <~ "}") ^^  (v => Unordered(v.toSet))

    lazy val zero: Parser[OrderingConstraint2] = "*" ^^^ Zero

    lazy val constraint: Parser[OrderingConstraint2] = ticVar | ordered | unordered | zero

    def parse(input: String): OrderingConstraint2 = parseAll(constraint, input).getOrElse(sys.error("Could not parse " + input))
  }

  def parse(input: String): OrderingConstraint2 = ConstraintParser.parse(input)

  "ordering constraint 2" >> {
    import OrderingConstraint2.{Join, Ordered, Unordered, Zero, Variable}

    "parse" should {
      "accept tic variable" in {
        c("'a").render mustEqual ("'a")
      }

      "accept empty seq" in {
        c("[]").render mustEqual ("[]")
      }

      "accept seq of tic variables" in {
        c("['a, 'b, 'c]").render mustEqual ("['a, 'b, 'c]")
      }

      "accept empty set" in {
        c("{}").render mustEqual ("{}")
      }

      "accept set of tic variables" in {
        c("{'a}").render mustEqual ("{'a}")
      }

      "accept sets and seqs combined" in {
        c("['a, ['b, {'d}], {'c}]").render mustEqual ("['a, ['b, {'d}], {'c}]") 
      }
    }

    "normalize" should {
      "remove empty seqs" in {
        c("['a, [], 'b]").normalize.render mustEqual "['a, 'b]"
      }

      "remove empty sets" in {
        c("['a, {}, 'b]").normalize.render mustEqual "['a, 'b]"
      }

      "collapse singleton seqs" in {
        c("['a, ['c], 'b]").normalize.render mustEqual "['a, 'c, 'b]"
      }

      "collapse singleton sets" in {
        c("['a, {'c}, 'b]").normalize.render mustEqual "['a, 'c, 'b]"
      }

      "collapse multiple level seqs" in {
        c("['a, [[['c]]], 'b]").normalize.render mustEqual "['a, 'c, 'b]"
      }

      "collapse multiple level sets" in {
        c("['a, {{{'c}}}, 'b]").normalize.render mustEqual "['a, 'c, 'b]"
      }

      "convert empty seq to zero" in {
        c("[[[]]]").normalize.render mustEqual "*"
      }

      "convert empty set to zero" in {
        c("{{{}}}").normalize.render mustEqual "*"
      }
    }

    "variables" should {
      "identify all variables" in {
        c("[{'a, 'b}, 'c, ['d, {'e, 'f}]]").variables mustEqual Set("a", "b", "c", "d", "e", "f").map(CPathField.apply)
      }
    }

    "-" should {
      "remove vars from nested expression" in {
        (c("[{'a, 'b}, 'c, ['d, {'e, 'f}]]") - c("['a, 'f]").fixed.toSet).render mustEqual c("['b, 'c, 'd, 'e]").render
      }
    }

    "join" should {
    //  "succeed for ['a, 'b] join ['a]" in {
    //    c("['a, 'b]").join(c("['a]")) mustEqual Join(c("'a"), leftRem = c("'b"), rightRem = Zero)
    //  }

    //  "succeed for ['a, 'b] join {'a}" in {
    //    c("['a, 'b]").join(c("{'a}")) mustEqual Join(c("'a"), leftRem = c("'b"), rightRem = Zero)
    //  }

    //  "succeed for ['a, 'b] join {'a, 'b}" in {
    //    c("['a, 'b]").join(c("{'a, 'b}")) mustEqual Join(c("['a, 'b]"), leftRem = Zero, rightRem = Zero)
    //  }

    //  "fail for ['a, 'b] join ['b]" in {
    //    c("['a, 'b]").join(c("['b]")) mustEqual Join(Zero, c("['a, 'b]"), c("'b"))
    //  }

    //  "succeed for ['a, 'b, 'c] join [{'a, 'b}, 'c]" in {
    //    c("['a, 'b, 'c]").join(c("[{'a, 'b}, 'c]")) mustEqual Join(c("['a, 'b, 'c]"))
    //  }

    //  "succeed for ['a, 'b, 'c] join [{'a, 'b}, 'd]" in {
    //    c("['a, 'b, 'c]").join(c("[{'a, 'b}, 'd]")) mustEqual Join(c("['a, 'b]"), leftRem = c("'c"), rightRem = c("'d"))
    //  }

    //  "succeed for ['b, 'c] join 'b" in {
    //    c("['b, 'c]").join(c("'b")) mustEqual Join(join = c("'b"), leftRem = c("'c"), rightRem = Zero)
    //  }

      "succeed for {'a, ['b, 'c], 'd} join {'a, 'b, ['c, 'd]}" in {
        val joined = c("{'a, ['b, 'c], 'd}").join(c("{'a, 'b, ['c, 'd]}"))

        println("join = " + joined.join.render + ", leftRem = " + joined.leftRem.render + ", rightRem = " + joined.rightRem.render)

        // join = 'a, leftRem = {'d, ['b, 'c]}, rightRem = {'b, ['c, 'd]}

        joined mustEqual Join(c("{'a, ['b, 'c, 'd]}"))
      }.pendingUntilFixed

    //  "succeed for {'a, 'b} join {'a, 'b, 'c}" in {
    //    c("{'a, 'b}").join(c("{'a, 'b, 'c}")) mustEqual Join(join = c("{'a, 'b}"), leftRem = Zero, rightRem = c("'c"))
    //  }
    }
  }

  "borg algorithm" >> {
    //val plan1 = BorgTraversalPlanUnfixed()
    "unfixed plan" should {
        val ab = mergeNode("ab")
        val abc = mergeNode("abc")
        val ad = mergeNode("ad")

        val connectedNodes = Set(ab, abc, ad)

        val spanningGraph = findSpanningGraphs(edgeMap(connectedNodes)).head

        val oracle = Map(
          ab -> NodeMetadata(10, None),
          abc -> NodeMetadata(10, None),
          ad -> NodeMetadata(10, None)
        )

        //val plan = findBorgTraversalOrder(spanningGraph, oracle)

        val steps = Vector.empty[BorgTraversalPlanUnfixed] //plan.unpack

//          val nodeOrder = steps.map(_.nodeOrder.render)
//          val accOrderPre = steps.map(_.accOrderPre.render)
//          val accOrderPost = steps.map(_.accOrderPost.render)
//          val accResort = steps.map(_.accResort)
//          val nodeResort = steps.map(_.nodeResort)


        steps.foreach { plan =>
          println("=========================")
          println("node = " + plan.node)
          println("nodeOrder = " + plan.nodeOrder.render)
          println("accOrderPre = " + plan.accOrderPre.render)
          println("accOrderPost = " + plan.accOrderPost.render)
          println("accResort = " + plan.accResort)
          println("nodeResort = " + plan.nodeResort)
        }
    }

//      "traversal ordering" >> {
//        "for ab-abc-ad" should {
//          val ab = mergeNode("ab")
//          val abc = mergeNode("abc")
//          val ad = mergeNode("ad")
//
//          val connectedNodes = Set(ab, abc, ad)
//
//          val spanningGraph = findSpanningGraphs(edgeMap(connectedNodes)).head
//
//          val oracle = Map(
//            ab -> NodeMetadata(10, None),
//            abc -> NodeMetadata(10, None),
//            ad -> NodeMetadata(10, None)
//          )
//
//          val plan = findBorgTraversalOrder(spanningGraph, oracle).fixed
//
//          val nodes = plan.steps.map(_.node)
//
//          val accOrdersPre = plan.steps.map(_.accOrderPre)
//          val accOrdersPost = plan.steps.map(_.accOrderPost)
//          val nodeOrders = plan.steps.map(_.nodeOrder)
//          val resorts = plan.steps.map(_.accResort)
//
//          val ad_abc_ab = Vector(ad, abc, ab)
//          val ad_ab_abc = Vector(ad, ab, abc)
//          val ab_abc_ad = Vector(ab, abc, ad)
//          val abc_ab_ad = Vector(abc, ab, ad)
//          val abc_ad_ab = Vector(abc, ad, ab)
//          val ab_ad_abc = Vector(ab, ad, abc)
//
//          //println(nodes)
//
//          "choose correct node order" in {
//            ((nodes == ad_abc_ab) ||
//             (nodes == ad_ab_abc) ||
//             (nodes == ab_abc_ad) ||
//             (nodes == ab_ad_abc) ||
//             (nodes == abc_ad_ab) ||
//             (nodes == abc_ab_ad)) must beTrue
//          }
//
//          "not require any acc resorts" in {
//            forall(resorts) { resort =>
//              resort must beFalse
//            }
//          }
//
//          "correctly order acc post and pre" in {
//            nodes match {
//              case `ad_abc_ab` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ad"), order("abdc"))
//                accOrdersPost must_== Vector(order("ad"), order("abdc"), order("abdc"))
//
//              case `ad_ab_abc` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ad"), order("abd"))
//                accOrdersPost must_== Vector(order("ad"), order("abd"), order("abcd"))
//
//              case `ab_abc_ad` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ab"), order("abc"))
//                accOrdersPost must_== Vector(order("ab"), order("abc"), order("abcd"))
//
//              case `ab_ad_abc` =>
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ab"), order("abd"))
//                accOrdersPost must_== Vector(order("ab"), order("abd"), order("abcd"))
//
//              case `abc_ab_ad` => 
//                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("abc"), order("abc"))
//                accOrdersPost must_== Vector(order("abc"), order("abc"), order("abcd"))
//
//              case `abc_ad_ab` => 
//                accOrdersPre must_== Vector(OrderingConstraint.Zero, order("abc"), order("abcd"))
//                accOrdersPost must_== Vector(order("abc"), order("abcd"), order("abcd"))
//            }
//          }
//
//          "correctly order nodes" in {
//            nodes match {
//              case `ad_abc_ab` => 
//                nodeOrders must_== Vector(order("ad"), order("abc"), order("ab"))
//
//              case `ab_abc_ad` => 
//                nodeOrders must_== Vector(order("ab"), order("abc"), order("ad"))
//                
//              case `abc_ab_ad` => 
//                nodeOrders must_== Vector(order("abc"), order("ab"), order("ad"))
//
//              case `abc_ad_ab` =>
//                nodeOrders must_== Vector(order("abc"), order("ad"), order("ab"))
//
//              case `ab_ad_abc` => 
//                nodeOrders must_== Vector(order("ab"), order("ad"), order("abc"))
//            }
//          }
//        }
//      }
//    }
*/
}


// vim: set ts=4 sw=4 et:
