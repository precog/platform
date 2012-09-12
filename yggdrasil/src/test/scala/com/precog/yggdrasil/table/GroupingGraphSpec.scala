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

import blueeyes.json._
import blueeyes.json.JsonAST._

import org.specs2.mutable._

import scalaz._
import scalaz.syntax.copointed._

import TableModule._

trait GroupingGraphSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  import trans._
  import trans.constants._

  import Table._
  import Table.Universe._
  import OrderingConstraints._
  import GroupKeyTrans._

  override type GroupId = Int

  def constraint(str: String) = OrderingConstraint(str.split(",").toSeq.map(_.toSet.map((c: Char) => JPathField(c.toString))))
  def ticvars(str: String): Seq[TicVar] = str.toSeq.map((c: Char) => JPathField(c.toString))
  def order(str: String) = OrderingConstraint.fromFixed(ticvars(str))
  def mergeNode(str: String) = MergeNode(ticvars(str).toSet, null)

  def testGenerateSingleBindingUniverse = {
    val spec = GroupingSource(
      Table.empty, 
      SourceKey.Single, Some(TransSpec1.Id), 2, 
      GroupKeySpecSource(JPathField("1"), TransSpec1.Id))

    Table.findBindingUniverses(spec) must haveSize(1)
  }

  def testGenSingleBindWithoutDisjunctions = {
    val spec = GroupingSource(
      Table.empty,
      SourceKey.Single, Some(SourceValue.Single), 3,
      GroupKeySpecAnd(
        GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
        GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))

    Table.findBindingUniverses(spec) must haveSize(1)
  }

  def testMultiSourceGenSingleBindWithoutDisjunctions = {
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

  def testSingleSourceMultiBindEqualDisjunctiveClauses = {
    val spec = GroupingSource(
      Table.empty,
      SourceKey.Single, Some(SourceValue.Single), 3,
      GroupKeySpecOr(
        GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
        GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))

    Table.findBindingUniverses(spec) must haveSize(2)
  }

  def testMultiSourceMultiBindEqualDisjunctiveClauses = {
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

  "derivation of the universes of binding constraints" should {
    "generate single binding universes for single-source groupings" in testGenerateSingleBindingUniverse
    "generate single binding universes if no disjunctions are present for single-source groupings" in testGenSingleBindWithoutDisjunctions
    "multiple-source groupings should generate single binding universes if no disjunctions are present" in testMultiSourceGenSingleBindWithoutDisjunctions
    "single-source groupings should generate a number of binding universes equal to the number of disjunctive clauses" in testSingleSourceMultiBindEqualDisjunctiveClauses
    "multiple-source groupings should generate a number of binding universes equal to the product of the number of disjunctive clauses from each source" in testMultiSourceMultiBindEqualDisjunctiveClauses
  }

  "Grouping graph support" should {
    "find the maximal spanning forest of a set of merge trees" in {
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

    "process binding constraints" >> {
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

    "transspec support" >> {
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


      "transform a group key transspec to use a desired sort key order" in {
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
  }
}

// vim: set ts=4 sw=4 et:
