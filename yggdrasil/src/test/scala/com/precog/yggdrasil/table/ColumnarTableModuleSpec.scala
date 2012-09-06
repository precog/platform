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
  TableModuleSpec[M] with
  CogroupSpec[M] with
  CrossSpec[M] with
  TestColumnarTableModule[M] with
  TransformSpec[M] with
  BlockLoadSpec[M] with
  BlockSortSpec[M] with
  CompactSpec[M] with 
  IntersectSpec[M] with
  DistinctSpec[M] { spec => //with
  //GrouperSpec[M] { spec =>

  type GroupId = Int
  import trans._
  import constants._
    
  override val defaultPrettyParams = Pretty.Params(2)

  val testPath = Path("/tableOpsSpec")
  val actorSystem = ActorSystem("columnar-table-specs")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  def lookupF1(namespace: List[String], name: String): F1 = {
    val lib = Map[String, CF1](
      "negate" -> cf.math.Negate,
      "true" -> new CF1P({ case _ => Column.const(true) })
    )

    lib(name)
  }

  def lookupF2(namespace: List[String], name: String): F2 = {
    val lib  = Map[String, CF2](
      "add" -> cf.math.Add,
      "mod" -> cf.math.Mod,
      "eq"  -> cf.std.Eq
    )
    lib(name)
  }

  def lookupScanner(namespace: List[String], name: String): CScanner = {
    val lib = Map[String, CScanner](
      "sum" -> new CScanner {
        type A = BigDecimal
        val init = BigDecimal(0)
        def scan(a: BigDecimal, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
          val prioritized = cols.values filter {
            case _: LongColumn | _: DoubleColumn | _: NumColumn => true
            case _ => false
          }
          
          val mask = BitSet(range filter { i => prioritized exists { _ isDefinedAt i } }: _*)
          
          val (a2, arr) = mask.foldLeft((a, new Array[BigDecimal](range.end))) {
            case ((acc, arr), i) => {
              val col = prioritized find { _ isDefinedAt i }
              
              val acc2 = col map {
                case lc: LongColumn =>
                  acc + lc(i)
                
                case dc: DoubleColumn =>
                  acc + dc(i)
                
                case nc: NumColumn =>
                  acc + nc(i)
              }
              
              acc2 foreach { arr(i) = _ }
              
              (acc2 getOrElse acc, arr)
            }
          }
          
          (a2, Map(ColumnRef(JPath.Identity, CNum) -> ArrayNumColumn(mask, arr)))
        }
      }
    )

    lib(name)
  }

  type Table = UnloadableTable
  class UnloadableTable(slices: StreamT[M, Slice]) extends ColumnarTable(slices) {
    import trans._
    def load(uid: UserId, jtpe: JType): M[Table] = sys.error("todo")
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder) = sys.error("todo")
  }
  
  type MemoContext = DummyMemoizationContext
  def newMemoContext = new DummyMemoizationContext

  trait TableCompanion extends ColumnarTableCompanion {
    implicit val geq: scalaz.Equal[Int] = intInstance

    def apply(slices: StreamT[M, Slice]) = new UnloadableTable(slices)

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): M[(Table, Table)] = 
      sys.error("not implemented here")
  }

  object Table extends TableCompanion

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

    "verify bijection from JSON" in checkMappings

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
      }.pendingUntilFixed
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
      "perform a less trivial type-based filter" in checkTyped.pendingUntilFixed
      "perform a summation scan case 1" in testTrivialScan
      "perform a summation scan" in checkScan.pendingUntilFixed
      "perform dynamic object deref" in testDerefObjectDynamic
      "perform an array swap" in checkArraySwap
      "replace defined rows with a constant" in checkConst
    }

    "in load" >> {
      "reconstruct a problem sample" in testLoadSample1
      "reconstruct a problem sample" in testLoadSample2
      "reconstruct a problem sample" in testLoadSample3
      "reconstruct a problem sample" in testLoadSample4
      //"reconstruct a problem sample" in testLoadSample5 //pathological sample in the case of duplicated ids.
      "reconstruct a dense dataset" in checkLoadDense
    }                           

    "sort" >> {
      "fully homogeneous data"        in homogeneousSortSample.pendingUntilFixed
      //"data with undefined sort keys" in partiallyUndefinedSortSample.pendingUntilFixed // throwing nasties that pendingUntilFixed doesn't catch
      "heterogeneous sort keys"       in heterogeneousSortSample.pendingUntilFixed
      "arbitrary datasets"            in checkSortDense.pendingUntilFixed
    }

    "intersect by identity" >> {
      "simple data" in testSimpleIntersect.pendingUntilFixed
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
      "have no duplicate rows" in testDistinct.pendingUntilFixed
    }
  }

  "grouping support" should {  
    import Table._
    import Table.Universe._
    def constraint(str: String) = OrderingConstraint(str.split(",").toSeq.map(_.toSet.map((c: Char) => JPathField(c.toString))))
    def ticvars(str: String) = str.toSeq.map((c: Char) => JPathField(c.toString))

    "traversal order" >> {
      "choose correct node order for ab-bc-ad" in {
        val ab = MergeNode(ticvars("ab").toSet, null)
        val abc = MergeNode(ticvars("bc").toSet, null)
        val ad = MergeNode(ticvars("ad").toSet, null)

        val connectedNodes = Set(ab, abc, ad)

        val spanningGraph = findSpanningGraphs(edgeMap(connectedNodes)).head

        val oracle = Map(
          ab -> NodeMetadata(10),
          abc -> NodeMetadata(10),
          ad -> NodeMetadata(10)
        )

        val plan = findBorgTraversalOrder(spanningGraph, oracle)

        val nodes = plan.steps.map(_.node)

        (nodes must_== Vector(ab, abc, ad)) or
        (nodes must_== Vector(abc, ab, ad))
      }
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

      /*
      "fix ordering" >> {
        "trivial case" in {
          fix(Set(constraint("a,b,c,d"))).map(_.toList) must_== Set(ticvars("abcd"))
        }

        "case with prior restrictions" in {
          val abc = constraint("ab,c")

          fix(Set(abc), Some(ticvars("ab"))).map(_.toList) must_== Set(ticvars("abc"))
        }

        "nontrivial case with prior restrictions" in {
          val minimized = Set(
            constraint("c,a,b,d"),
            constraint("ab")
          )

          val expected = Set(
            ticvars("cabd"),
            ticvars("ab")
          )

          fix(minimized, Some(ticvars("ab"))) must_== expected
        }

        "error if hint cannot be respected" in {
          val minimized = Set(
            constraint("c,a,b,d"),
            constraint("ab")
          )

          fix(minimized, Some(ticvars("ac"))) must throwA[RuntimeException]
        }
      }
      */
    }

    "graph traversal" >> {
      def norm(s: Set[OrderingConstraint]) = s.map(_.ordering.toList)

      val abcd = MergeNode(ticvars("abcd").toSet, null)
      val abc = MergeNode(ticvars("abc").toSet, null)
      val ab = MergeNode(ticvars("ab").toSet, null)
      val ac = MergeNode(ticvars("ac").toSet, null)
      val a = MergeNode(ticvars("a").toSet, null)

      /*
      "find underconstrained binding constraints" >> {
        "for a graph with a supernode" in {
          val allNodes = Random.shuffle(Set(abcd, abc, ab, ac, a))

          val spanningForest = findSpanningGraphs(edgeMap(allNodes))

          spanningForest must haveSize(1)

          val underconstrained = spanningForest.head.underconstrained

          norm(underconstrained(a)) must_== norm(Set(constraint("a")))
          norm(underconstrained(ab)) must_== norm(Set(constraint("ab")))
          norm(underconstrained(ac)) must_== norm(Set(constraint("ac")))
          norm(underconstrained(abc)) must_== norm(Set(constraint("abc")))
          norm(underconstrained(abcd)) must_== norm(Set(constraint("a,bcd"), constraint("ab,cd"), constraint("ac,bd"), constraint("abc,d")))
        }

        "for a graph without a supernode" in {
          val abd = MergeNode(ticvars("abd").toSet, null)
          val allNodes = Random.shuffle(Set(abd, abc, ab))

          val spanningForest = findSpanningGraphs(edgeMap(allNodes))

          spanningForest must haveSize(1)

          val underconstrained = spanningForest.head.underconstrained

          norm(underconstrained(ab)) must_== norm(Set(constraint("ab")))
          norm(underconstrained(abc)) must_== norm(Set(constraint("abc"), constraint("ab,c")))
          norm(underconstrained(abd)) must_== norm(Set(constraint("abd"), constraint("ab,d")))
        }
      }
      */
    }

/*
    "select constraint matching a set of binding constraints" >> {
      "a preferred match" in {
        val preferred = Set(ticvars("abc"), ticvars("abd"))

        val constraints = Set(
          constraint("ab,c"),
          constraint("a,c,b")
        )

        OrderingConstraints.select(preferred, constraints) must beSome(ticvars("abc"))
      }

      "error on no match" in {
        val preferred = Set(ticvars("abce"), ticvars("abd"))

        val constraints = Set(
          constraint("ab,c"),
          constraint("a,c,b")
        )

        OrderingConstraints.select(preferred, constraints) must beNone
      }
    }

    "generate a trivial merge specification" in {
      // Query:
      // forall 'a 
      //   foo' := foo where foo.a = 'a

      val ticvar = JPathField("a")
      val tree = MergeGraph(Set(node))
      val binding = Binding(Table.empty, SourceKey.Single, Some(TransSpec1.Id), 1, GroupKeySpecSource(ticvar, DerefObjectStatic(SourceValue.Single, ticvar)))
      val node = MergeNode(Set(ticvar), binding)

      val result = buildMerges(Map(node -> List(binding)), tree)

      val expected = NodeMergeSpec(
        List(ticvar),
        Set(
          SourceMergeSpec(
            binding,
            ObjectConcat(WrapObject(DerefObjectStatic(SourceValue.Single, ticvar), "0")),
            List(ticvar)
          )))
      
      result must_== expected
    }

    "generate a merge specification" in {
      // Query:
      // forall 'a forall 'b
      //   foo' := foo where foo.a = 'a
      //   bar' := bar where bar.a = 'a & bar.b = 'b

      val tica = JPathField("a")
      val ticb = JPathField("b")
      val foonode = MergeNode(Set(tica))
      val barnode = MergeNode(Set(tica, ticb))
      val tree = MergeGraph(Set(foonode, barnode), Set(MergeEdge(foonode, barnode, Set(tica))))
      val foobinding = Binding(Table.empty, SourceKey.Single, TransSpec1.Id, 1, GroupKeySpecSource(tica, DerefObjectStatic(SourceValue.Single, tica)))
      val barbinding = Binding(Table.empty, SourceKey.Single, TransSpec1.Id, 1, 
        GroupKeySpecAnd(
          GroupKeySpecSource(tica, DerefObjectStatic(SourceValue.Single, tica)),
          GroupKeySpecSource(ticb, DerefObjectStatic(SourceValue.Single, ticb))))

      val result = buildMerges(Map(foonode -> List(foobinding), barnode -> List(barbinding)), tree)

      val expected = NodeMergeSpec(
        Vector(tica),
        Set(
          LeftAlignMergeSpec(
            MergeAlignment(
              SourceMergeSpec(
                foobinding, 
                ObjectConcat(WrapObject(DerefObjectStatic(SourceValue.Single,tica), "0")),
                List(tica)
              ),
              NodeMergeSpec(
                List(tica, ticb),
                Set(
                  SourceMergeSpec(
                    barbinding,
                    ObjectConcat(
                      WrapObject(DerefObjectStatic(SourceValue.Single,tica), "0"),
                      WrapObject(DerefObjectStatic(SourceValue.Single,ticb), "1")),
                    List(tica, ticb)
                  ))),
              Vector(tica)
            ))))

      result must_== expected
    }
    */

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
