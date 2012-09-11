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
import scala.collection.BitSet
import scala.collection.mutable.LinkedHashSet
import scala.util.Random

import scalaz._
import scalaz.effect.IO 
import scalaz.syntax.copointed._
import scalaz.std.anyVal._
import scalaz.std.stream._

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

import TableModule._

trait ColumnarTableModuleSpec[M[+_]] extends
    ColumnarTableModuleTestSupport[M] with
    TableModuleSpec[M] with
    CogroupSpec[M] with
    CrossSpec[M] with
    TransformSpec[M] with
    CompactSpec[M] with 
    PartitionMergeSpec[M] with
    DistinctSpec[M] { spec => //with
    //GrouperSpec[M] { spec =>

  type GroupId = Int
  import trans._
  import constants._
    
  override val defaultPrettyParams = Pretty.Params(2)

  class MemoContext extends MemoizationContext {
    import trans._
    
    def memoize(table: Table, memoId: MemoId): M[Table] = M.point(table)
    def sort(table: Table, sortKey: TransSpec1, sortOrder: DesiredSortOrder, memoId: MemoId, unique: Boolean = true): M[Table] =
      table.sort(sortKey, sortOrder)
    
    def expire(memoId: MemoId): Unit = ()
    def purge(): Unit = ()
  }

  class Table(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import trans._
    def load(uid: UserId, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean = true) = sys.error("todo")
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder = SortAscending, unique: Boolean = true): M[Seq[Table]] = sys.error("todo")
  }
  
  trait TableCompanion extends ColumnarTableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance

    def apply(slices: StreamT[M, Slice]) = new Table(slices)

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = 
      sys.error("not implemented here")
  }

  object Table extends TableCompanion
  def newMemoContext = new MemoContext

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample: List[JValue] = List(
        JObject(
          JField("key", JArray(JNum(-1L) :: JNum(0L) :: Nil)) ::
          JField("value", JNull) :: Nil
        ), 
        JObject(
          JField("key", JArray(JNum(-3090012080927607325l) :: JNum(2875286661755661474l) :: Nil)) ::
          JField("value", JObject(List(
            JField("q8b", JArray(List(
              JNum(6.615224799778253E307d), 
              JArray(List(JBool(false), JNull, JNum(-8.988465674311579E307d))), JNum(-3.536399224770604E307d)))), 
            JField("lwu",JNum(-5.121099465699862E307d))))
          ) :: Nil
        ), 
        JObject(
          JField("key", JArray(JNum(-3918416808128018609l) :: JNum(-1L) :: Nil)) ::
          JField("value", JNum(-1.0)) :: Nil
        )
      )

      val dataset = fromJson(sample.toStream)
      val results = dataset.toJson
      results.copoint must containAllOf(sample).only 
    }

    "verify bijection from JSON" in checkMappings(this)

    "in cogroup" >> {
      "perform a simple cogroup" in testSimpleCogroup
      "perform another simple cogroup" in testAnotherSimpleCogroup
      "perform yet another simple cogroup" in testAnotherSimpleCogroupSwitched
      "cogroup across slice boundaries" in testCogroupSliceBoundaries
      "error on unsorted inputs" in testUnsortedInputs

      "survive pathology 1" in testCogroupPathology1
      "survive pathology 2" in testCogroupPathology2
      "survive pathology 3" in testCogroupPathology3
      
      "survive scalacheck" in { 
        check { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) } 
      }
    }

    "in cross" >> {
      "perform a simple cartesian" in testSimpleCross
      "cross across slice boundaries on one side" in testCrossSingles
      "survive scalacheck" in {
        check { cogroupData: (SampleData, SampleData) => testCross(cogroupData._1, cogroupData._2) } 
      }
    }

    "in transform" >> {
      "perform the identity transform" in checkTransformLeaf
      "perform a trivial map1" in testMap1IntLeaf
      //"give the identity transform for the trivial filter" in checkTrivialFilter  //why is this commented out?
      "give the identity transform for the trivial 'true' filter" in checkTrueFilter
      "give the identity transform for a nontrivial filter" in checkFilter
      "perform an object dereference" in checkObjectDeref
      "perform an array dereference" in checkArrayDeref
      "perform a trivial map2" in checkMap2
      "perform a trivial equality check" in checkEqualSelf
      "perform a slightly less trivial equality check" in checkEqual
      "wrap the results of a transform in an object as the specified field" in checkWrapObject
      "give the identity transform for self-object concatenation" in checkObjectConcatSelf
      "use a right-biased overwrite strategy in object concat conflicts" in checkObjectConcatOverwrite
      "concatenate dissimilar objects" in checkObjectConcat
      "concatenate dissimilar arrays" in checkArrayConcat
      "delete elements according to a JType" in checkObjectDelete
      "perform a trivial type-based filter" in checkTypedTrivial
      "perform a trivial heterogeneous type-based filter" in checkTypedHeterogeneous
      "perform a trivial object type-based filter" in checkTypedObject
      "perform another trivial object type-based filter" in checkTypedObject2
      "perform a trivial array type-based filter" in checkTypedArray
      "perform another trivial array type-based filter" in checkTypedArray2
      "perform yet another trivial array type-based filter" in checkTypedArray3
      "perform a fourth trivial array type-based filter" in checkTypedArray4
      "perform a trivial number type-based filter" in checkTypedNumber
      "perform another trivial number type-based filter" in checkTypedNumber2
      "perform a filter returning the empty set" in checkTypedEmpty
      "perform a less trivial type-based filter" in checkTyped
      "perform a summation scan case 1" in testTrivialScan
      "perform a summation scan of heterogeneous data" in testHetScan
      "perform a summation scan" in checkScan
      "perform dynamic object deref" in testDerefObjectDynamic
      "perform an array swap" in checkArraySwap
      "replace defined rows with a constant" in checkConst
    }

    "in compact" >> {
      "be the identity on fully defined tables"  in testCompactIdentity
      "preserve all defined rows"                in testCompactPreserve
      "have no undefined rows"                   in testCompactRows
      "have no empty slices"                     in testCompactSlices
      "preserve all defined key rows"            in testCompactPreserveKey
      "have no undefined key rows"               in testCompactRowsKey
      "have no empty key slices"                 in testCompactSlicesKey
    }
    
    "in distinct" >> {
      "be the identity on tables with no duplicate rows" in testDistinctIdentity
      "peform properly when the same row appears in two different slices" in testDistinctAcrossSlices
      "peform properly again when the same row appears in two different slices" in testDistinctAcrossSlices2
      "have no duplicate rows" in testDistinct
    }
  }

  "grouping support" >> {  
    import Table._
    import Table.Universe._
    def constraint(str: String) = OrderingConstraint(str.split(",").toSeq.map(_.toSet.map((c: Char) => JPathField(c.toString))))
    def c(str: String) = OrderingConstraint2.parse(str)
    def ticvars(str: String) = str.toSeq.map((c: Char) => JPathField(c.toString))
    def order(str: String) = OrderingConstraint.fromFixed(ticvars(str))
    def mergeNode(str: String) = MergeNode(ticvars(str).toSet, null)

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
          c("[{'a, 'b}, 'c, ['d, {'e, 'f}]]").variables mustEqual Set("a", "b", "c", "d", "e", "f").map(JPathField.apply)
        }
      }

      "-" should {
        "remove vars from nested expression" in {
          (c("[{'a, 'b}, 'c, ['d, {'e, 'f}]]") - c("['a, 'f]").fixed.toSet).render mustEqual c("['b, 'c, 'd, 'e]").render
        }
      }

      "join" should {
        "succeed for ['a, 'b] join ['a]" in {
          c("['a, 'b]").join(c("['a]")) mustEqual Join(c("'a"), leftRem = c("'b"), rightRem = Zero)
        }

        "succeed for ['a, 'b] join {'a}" in {
          c("['a, 'b]").join(c("{'a}")) mustEqual Join(c("'a"), leftRem = c("'b"), rightRem = Zero)
        }

        "succeed for ['a, 'b] join {'a, 'b}" in {
          c("['a, 'b]").join(c("{'a, 'b}")) mustEqual Join(c("['a, 'b]"), leftRem = Zero, rightRem = Zero)
        }

        "fail for ['a, 'b] join ['b]" in {
          c("['a, 'b]").join(c("['b]")) mustEqual Join(Zero, c("['a, 'b]"), c("'b"))
        }

        "succeed for ['a, 'b, 'c] join [{'a, 'b}, 'c]" in {
          c("['a, 'b, 'c]").join(c("[{'a, 'b}, 'c]")) mustEqual Join(c("['a, 'b, 'c]"))
        }

        "succeed for ['a, 'b, 'c] join [{'a, 'b}, 'd]" in {
          c("['a, 'b, 'c]").join(c("[{'a, 'b}, 'd]")) mustEqual Join(c("['a, 'b]"), leftRem = c("'c"), rightRem = c("'d"))
        }

        "succeed for {'a, ['b, 'c], 'd} join {'a, 'b, ['c, 'd]}" in {
          val joined = c("{'a, ['b, 'c], 'd}").join(c("{'a, 'b, ['c, 'd]}"))

          println("join = " + joined.join.render + ", leftRem = " + joined.leftRem.render + ", rightRem = " + joined.rightRem.render)

          // join = 'a, leftRem = {'d, ['b, 'c]}, rightRem = *

          joined mustEqual Join(c("{'a, ['b, 'c, 'd]}"))
        }.pendingUntilFixed

        "succeed for {'a, 'b} join {'a, 'b, 'c}" in {
          c("{'a, 'b}").join(c("{'a, 'b, 'c}")) mustEqual Join(join = c("{'a, 'b}"), leftRem = Zero, rightRem = c("'c"))
        }
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

          val plan = findBorgTraversalOrder(spanningGraph, oracle)

          val steps = plan.unpack

          /*val nodeOrder = steps.map(_.nodeOrder.render)
          val accOrderPre = steps.map(_.accOrderPre.render)
          val accOrderPost = steps.map(_.accOrderPost.render)
          val accResort = steps.map(_.accResort)
          val nodeResort = steps.map(_.nodeResort)*/

          /*

          */

          steps.foreach { plan =>
            println("=========================")
            println("node = " + plan.node)
            println("nodeOrder = " + plan.nodeOrder.render)
            println("accOrderPre = " + plan.accOrderPre.render)
            println("accOrderPost = " + plan.accOrderPost.render)
            println("accResort = " + plan.accResort)
            println("nodeResort = " + plan.nodeResort)
          }



          /*

          BorgTraversalPlanUnfixed(None,MergeNode(Set(.a, .b),null),Zero,Zero,BorgTraversalCostModel(0,0,Set()),false,false)
          
          BorgTraversalPlanUnfixed(None,MergeNode(Set(.a, .b, .c),null),Ordered(Vector(Unordered(Set()), Unordered(Set(Variable(.a), Variable(.b), Variable(.c))))),Ordered(Vector(Unordered(Set(Variable(.a), Variable(.b), Variable(.c))), Unordered(Set(Zero)))),BorgTraversalCostModel(30,40,Set(.a, .b, .c)),false,true)

          BorgTraversalPlanUnfixed(None,MergeNode(Set(.a, .d),null),Ordered(Vector(Unordered(Set()), Unordered(Set(Variable(.a), Variable(.d))))),Ordered(Vector(Unordered(Set(Variable(.a), Variable(.d))), Unordered(Set(Zero)))),BorgTraversalCostModel(60,160,Set(.a, .b, .c, .d)),false,true)


          */
      }

      /* "traversal ordering" >> {
        "for ab-abc-ad" should {
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

          val plan = findBorgTraversalOrder(spanningGraph, oracle).fixed

          val nodes = plan.steps.map(_.node)

          val accOrdersPre = plan.steps.map(_.accOrderPre)
          val accOrdersPost = plan.steps.map(_.accOrderPost)
          val nodeOrders = plan.steps.map(_.nodeOrder)
          val resorts = plan.steps.map(_.accResort)

          val ad_abc_ab = Vector(ad, abc, ab)
          val ad_ab_abc = Vector(ad, ab, abc)
          val ab_abc_ad = Vector(ab, abc, ad)
          val abc_ab_ad = Vector(abc, ab, ad)
          val abc_ad_ab = Vector(abc, ad, ab)
          val ab_ad_abc = Vector(ab, ad, abc)

          //println(nodes)

          "choose correct node order" in {
            ((nodes == ad_abc_ab) ||
             (nodes == ad_ab_abc) ||
             (nodes == ab_abc_ad) ||
             (nodes == ab_ad_abc) ||
             (nodes == abc_ad_ab) ||
             (nodes == abc_ab_ad)) must beTrue
          }

          "not require any acc resorts" in {
            forall(resorts) { resort =>
              resort must beFalse
            }
          }

          "correctly order acc post and pre" in {
            nodes match {
              case `ad_abc_ab` => 
                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ad"), order("abdc"))
                accOrdersPost must_== Vector(order("ad"), order("abdc"), order("abdc"))

              case `ad_ab_abc` => 
                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ad"), order("abd"))
                accOrdersPost must_== Vector(order("ad"), order("abd"), order("abcd"))

              case `ab_abc_ad` => 
                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ab"), order("abc"))
                accOrdersPost must_== Vector(order("ab"), order("abc"), order("abcd"))

              case `ab_ad_abc` =>
                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("ab"), order("abd"))
                accOrdersPost must_== Vector(order("ab"), order("abd"), order("abcd"))

              case `abc_ab_ad` => 
                accOrdersPre must_==  Vector(OrderingConstraint.Zero, order("abc"), order("abc"))
                accOrdersPost must_== Vector(order("abc"), order("abc"), order("abcd"))

              case `abc_ad_ab` => 
                accOrdersPre must_== Vector(OrderingConstraint.Zero, order("abc"), order("abcd"))
                accOrdersPost must_== Vector(order("abc"), order("abcd"), order("abcd"))
            }
          }

          "correctly order nodes" in {
            nodes match {
              case `ad_abc_ab` => 
                nodeOrders must_== Vector(order("ad"), order("abc"), order("ab"))

              case `ab_abc_ad` => 
                nodeOrders must_== Vector(order("ab"), order("abc"), order("ad"))
                
              case `abc_ab_ad` => 
                nodeOrders must_== Vector(order("abc"), order("ab"), order("ad"))

              case `abc_ad_ab` =>
                nodeOrders must_== Vector(order("abc"), order("ad"), order("ab"))

              case `ab_ad_abc` => 
                nodeOrders must_== Vector(order("ab"), order("ad"), order("abc"))
            }
          }
        }
      }*/
    }

    "derive the universes of binding constraints" >> {
      "single-source groupings should generate single binding universes" in {
        val spec = GroupingSource(
          Table.empty, 
          SourceKey.Single, Some(TransSpec1.Id), 2, 
          GroupKeySpecSource(JPathField("1"), TransSpec1.Id))

        Table.findBindingUniverses(spec) must haveSize(1)
      }
      
      "single-source groupings should generate single binding universes if no disjunctions are present" in {
        val spec = GroupingSource(
          Table.empty,
          SourceKey.Single, Some(SourceValue.Single), 3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))

        Table.findBindingUniverses(spec) must haveSize(1)
      }
      
      "multiple-source groupings should generate single binding universes if no disjunctions are present" in {
        val spec1 = GroupingSource(
          Table.empty,
          SourceKey.Single, Some(TransSpec1.Id), 2,
          GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
          
        val spec2 = GroupingSource(
          Table.empty,
          SourceKey.Single, Some(TransSpec1.Id), 3,
          GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
          
        val union = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2, GroupingSpec.Union)

        Table.findBindingUniverses(union) must haveSize(1)
      }

      "single-source groupings should generate a number of binding universes equal to the number of disjunctive clauses" in {
        val spec = GroupingSource(
          Table.empty,
          SourceKey.Single, Some(SourceValue.Single), 3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))

        Table.findBindingUniverses(spec) must haveSize(2)
      }
      
      "multiple-source groupings should generate a number of binding universes equal to the product of the number of disjunctive clauses from each source" in {
        val spec1 = GroupingSource(
          Table.empty,
          SourceKey.Single, Some(TransSpec1.Id), 2,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = GroupingSource(
          Table.empty,
          SourceKey.Single, Some(TransSpec1.Id), 3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val union = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2, GroupingSpec.Union)

        Table.findBindingUniverses(union) must haveSize(4)
      }
    }

    "derive a correct TransSpec for a conjunctive GroupKeySpec" in {
      val keySpec = GroupKeySpecAnd(
        GroupKeySpecAnd(
          GroupKeySpecSource(JPathField("tica"), DerefObjectStatic(SourceValue.Single, JPathField("a"))),
          GroupKeySpecSource(JPathField("ticb"), DerefObjectStatic(SourceValue.Single, JPathField("b")))),
        GroupKeySpecSource(JPathField("ticc"), DerefObjectStatic(SourceValue.Single, JPathField("c"))))

      val transspec = GroupKeyTrans(Table.Universe.sources(keySpec))
      val JArray(data) = JsonParser.parse("""[
        {"key": [1], "value": {"a": 12, "b": 7}},
        {"key": [2], "value": {"a": 42}},
        {"key": [1], "value": {"a": 13, "c": true}}
      ]""")

      val JArray(expected) = JsonParser.parse("""[
        {"000000": 12, "000001": 7},
        {"000000": 42},
        {"000000": 13, "000002": true}
      ]""")

      fromJson(data.toStream).transform(transspec.spec).toJson.copoint must_== expected
    }

    "find the maximal spanning forest of a set of merge trees" in {
      import Table.Universe._

      val abcd = MergeNode(ticvars("abcd").toSet, null)
      val abc = MergeNode(ticvars("abc").toSet, null)
      val ab = MergeNode(ticvars("ab").toSet, null)
      val ac = MergeNode(ticvars("ac").toSet, null)
      val a = MergeNode(ticvars("a").toSet, null)
      val e = MergeNode(ticvars("e").toSet, null)

      val connectedNodes = Set(abcd, abc, ab, ac, a)
      val allNodes = connectedNodes + e
      val result = findSpanningGraphs(edgeMap(allNodes))

      result.toList must beLike {
        case MergeGraph(n1, e1) :: MergeGraph(n2, e2) :: Nil =>
          val (nodes, edges) = if (n1 == Set(e)) (n2, e2) else (n1, e1)

          nodes must haveSize(5)
          edges must haveSize(4) 
          edges.map(_.sharedKeys.size) must_== Set(3, 2, 2, 1)
      }
    }

    "find the maximal spanning forest of a set of merge trees" in {
      import Table.Universe._

      val ab = MergeNode(ticvars("ab").toSet, null)
      val bc = MergeNode(ticvars("bc").toSet, null)
      val ac = MergeNode(ticvars("ac").toSet, null)

      val connectedNodes = Set(ab, bc, ac)
      val result = findSpanningGraphs(edgeMap(connectedNodes))

      result must haveSize(1)
      result.head.nodes must_== connectedNodes

      val expectedUnorderedEdges = edgeMap(connectedNodes).values.flatten.toSet
      forall(result.head.edges) { edge =>
        (expectedUnorderedEdges must contain(edge)) //or
        //(expectedUnorderedEdges must contain(edge.reverse))
      }
    }

    "binding constraints" >> {
      import Table.OrderingConstraints._

      "minimize" >> {
        "minimize to multiple sets" in {
          val abcd = constraint("abcd")
          val abc = constraint("abc")
          val ab = constraint("ab")
          val ac = constraint("ac")

          val expected = Set(
            constraint("ab,c,d"),
            constraint("ac")
          )

          minimize(Set(abcd, abc, ab, ac)) must_== expected
        }

        "minimize to multiple sets with a singleton" in {
          val abcd = constraint("abcd")
          val abc = constraint("abc")
          val ab = constraint("ab")
          val ac = constraint("ac")
          val c = constraint("c")

          val expected = Set(
            constraint("c,a,b,d"),
            constraint("ab")
          )

          minimize(Set(abcd, abc, ab, ac, c)) must_== expected
        }

        "not minimize completely disjoint constraints" in {
          val ab = constraint("ab")
          val bc = constraint("bc")
          val ca = constraint("ca")

          val expected = Set(
            constraint("ab"),
            constraint("bc"),
            constraint("ca")
          )

          minimize(Set(ab, bc, ca)) must_== expected
        }
      }

      "find required sorts" >> {
        "simple sort" in {
          val abcd = MergeNode(ticvars("abcd").toSet, null)
          val abc = MergeNode(ticvars("abc").toSet, null)
          val ab = MergeNode(ticvars("ab").toSet, null)
          val ac = MergeNode(ticvars("ac").toSet, null)
          val a = MergeNode(ticvars("a").toSet, null)

          val spanningGraph = findSpanningGraphs(edgeMap(Set(abcd, abc, ab, ac, a))).head

          def checkPermutation(nodeList: List[MergeNode]) = {
            val requiredSorts = findRequiredSorts(spanningGraph, nodeList)

            requiredSorts(a) must_== Set(ticvars("a"))
            requiredSorts(ac) must_== Set(ticvars("ac"))
            requiredSorts(ab) must_== Set(ticvars("ab"))
            (requiredSorts(abc), requiredSorts(abcd)) must beLike {
              case (sabc, sabcd) =>
                (
                  (sabc == Set(ticvars("abc")) && (sabcd == Set(ticvars("abc"), ticvars("ac")))) ||
                  (sabc == Set(ticvars("acb")) && (sabcd == Set(ticvars("acb"), ticvars("ab")))) ||
                  (sabc == Set(ticvars("abc"), ticvars("ac")) && (sabcd == Set(ticvars("abc")))) ||
                  (sabc == Set(ticvars("acb"), ticvars("ab")) && (sabcd == Set(ticvars("acb")))) 
                ) must beTrue
            }
          }

          forall(spanningGraph.nodes.toList.permutations) { nodeList =>
            checkPermutation(nodeList)
          }
        }

        "in a cycle" in {
          val ab = MergeNode(ticvars("ab").toSet, null)
          val ac = MergeNode(ticvars("ac").toSet, null)
          val bc = MergeNode(ticvars("bc").toSet, null)

          val spanningGraph = findSpanningGraphs(edgeMap(Set(ab, ac, bc))).head

          forall(spanningGraph.nodes.toList.permutations) { nodeList =>
            val requiredSorts = findRequiredSorts(spanningGraph, nodeList)

            requiredSorts(ab) must_== Set(ticvars("a"), ticvars("b"))
            requiredSorts(ac) must_== Set(ticvars("a"), ticvars("c"))
            requiredSorts(bc) must_== Set(ticvars("b"), ticvars("c"))
          }
        }

        "in connected cycles" in {
          val ab = MergeNode(ticvars("ab").toSet, null)
          val ac = MergeNode(ticvars("ac").toSet, null)
          val bc = MergeNode(ticvars("bc").toSet, null)
          val ad = MergeNode(ticvars("ad").toSet, null)
          val db = MergeNode(ticvars("db").toSet, null)

          val spanningGraph = findSpanningGraphs(edgeMap(Set(ab, ac, bc, ad, db))).head

          forall(spanningGraph.nodes.toList.permutations) { nodeList =>
            val requiredSorts = findRequiredSorts(spanningGraph, nodeList)

            requiredSorts(ab) must_== Set(ticvars("a"), ticvars("b"))
            requiredSorts(ac) must_== Set(ticvars("a"), ticvars("c"))
            requiredSorts(bc) must_== Set(ticvars("b"), ticvars("c"))
            requiredSorts(ad) must_== Set(ticvars("a"), ticvars("d"))
            requiredSorts(db) must_== Set(ticvars("d"), ticvars("b"))
          }
        }

        "in a connected cycle with extraneous constraints" in {
          val ab = MergeNode(ticvars("ab").toSet, null)
          val ac = MergeNode(ticvars("ac").toSet, null)
          val bc = MergeNode(ticvars("bc").toSet, null)
          val ad = MergeNode(ticvars("ad").toSet, null)

          val spanningGraph = findSpanningGraphs(edgeMap(Set(ab, ac, bc, ad))).head

          forall(spanningGraph.nodes.toList.permutations) { nodeList =>
            val requiredSorts = findRequiredSorts(spanningGraph, nodeList)

            requiredSorts(ab) must_== Set(ticvars("a"), ticvars("b"))
            requiredSorts(ac) must_== Set(ticvars("a"), ticvars("c"))
            requiredSorts(bc) must_== Set(ticvars("b"), ticvars("c"))
            requiredSorts(ad) must_== Set(ticvars("a"))
          }
        }
      }
    }

    "transform a group key transspec to use a desired sort key order" in {
      import GroupKeyTrans._

      val trans = GroupKeyTrans(
        ObjectConcat(
          WrapObject(DerefObjectStatic(SourceValue.Single, JPathField("a")), keyName(0)),
          WrapObject(DerefObjectStatic(SourceValue.Single, JPathField("b")), keyName(1)),
          WrapObject(DerefObjectStatic(SourceValue.Single, JPathField("c")), keyName(2))
        ),
        ticvars("abc")
      )

      val JArray(data) = JsonParser.parse("""[
        {"key": [1], "value": {"a": 12, "b": 7}},
        {"key": [2], "value": {"a": 42}},
        {"key": [1], "value": {"a": 13, "c": true}}
      ]""")

      val JArray(expected) = JsonParser.parse("""[
        {"000001": 12, "000002": 7},
        {"000001": 42},
        {"000001": 13, "000000": true}
      ]""")

      val alignedSpec = trans.alignTo(ticvars("ca")).spec
      fromJson(data.toStream).transform(alignedSpec).toJson.copoint must_== expected
    }
  }

  "partitionMerge" should {
    "concatenate reductions of subsequences" in testPartitionMerge
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad

  type YggConfig = IdSourceConfig
  val yggConfig = new IdSourceConfig {
    val idSource = new IdSource {
      private val source = new java.util.concurrent.atomic.AtomicLong
      def nextId() = source.getAndIncrement
    }
  }
}


// vim: set ts=4 sw=4 et:
