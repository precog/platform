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

import akka.actor.ActorSystem
import akka.dispatch._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

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

trait ColumnarTableModuleSpec[M[+_]] extends
  TableModuleSpec[M] with
  CogroupSpec[M] with
  CrossSpec[M] with
  TestColumnarTableModule[M] with
  TransformSpec[M] with
  BlockLoadSpec[M] with
  BlockSortSpec[M] with
  CompactSpec[M] with 
  DistinctSpec[M] { spec => //with
  //GrouperSpec[M] { spec =>

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
        def scan(a: BigDecimal, col: Column, range: Range): (A, Option[Column]) = {
          col match {
            case lc: LongColumn => 
              val (a0, acc) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
                case ((a0, acc), i) => 
                  val intermediate = a0 + lc(i)
                  acc(i) = intermediate
                  (intermediate, acc)
              }

              (a0, Some(ArrayNumColumn(BitSet(range: _*), acc)))
              
            case lc: DoubleColumn =>
              val (a0, acc) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
                case ((a0, acc), i) => 
                  val intermediate = a0 + lc(i)
                  acc(i) = intermediate
                  (intermediate, acc)
              }

              (a0, Some(ArrayNumColumn(BitSet(range: _*), acc)))
              
            case lc: NumColumn =>
              val (a0, acc) = range.foldLeft((a, new Array[BigDecimal](range.end))) {
                case ((a0, acc), i) => 
                  val intermediate = a0 + lc(i)
                  acc(i) = intermediate
                  (intermediate, acc)
              }

              (a0, Some(ArrayNumColumn(BitSet(range: _*), acc)))

            case _ => (a, None)
          }
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

  def table(slices: StreamT[M, Slice]) = new UnloadableTable(slices)

  /*
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
      //dataset.slices.foreach(println)
      val results = dataset.toJson
      results.copoint must containAllOf(sample).only 
    }

    "verify bijection from JSON" in checkMappings

    "in cogroup" >> {
      "perform a simple cogroup" in testSimpleCogroup
      "cogroup across slice boundaries" in testCogroupSliceBoundaries

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
      //"give the identity transform for the trivial filter" in checkTrivialFilter
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
      "perform a less trivial type-based filter" in checkTyped
      "perform a summation scan" in checkScan
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
      "fully homogeneous data"        in homogeneousSortSample
      "data with undefined sort keys" in partiallyUndefinedSortSample
      "heterogeneous sort keys"       in heterogeneousSortSample
      "arbitrary datasets"            in checkSortDense
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
      "have no duplicate rows" in testDistinct
    }
  }
  */

  "grouping support" should {  
    import grouper._
    import grouper.Universe._
    def constraint(str: String) = BindingConstraint(str.split(",").toSeq.map(_.toSet.map((c: Char) => JPathField(c.toString))))
    def ticvars(str: String) = str.toSeq.map((c: Char) => JPathField(c.toString))

    "derive the universes of binding constraints" >> {
      "single-source groupings should generate single binding universes" in {
        val spec = GroupingSource(
          ops.empty, 
          SourceKey.Single, TransSpec1.Id, 2, 
          GroupKeySpecSource(JPathField("1"), TransSpec1.Id))

        grouper.findBindingUniverses(spec) must haveSize(1)
      }
      
      "single-source groupings should generate single binding universes if no disjunctions are present" in {
        val spec = GroupingSource(
          ops.empty,
          SourceKey.Single, SourceValue.Single, 3,
          GroupKeySpecAnd(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))

        grouper.findBindingUniverses(spec) must haveSize(1)
      }
      
      "multiple-source groupings should generate single binding universes if no disjunctions are present" in {
        val spec1 = GroupingSource(
          ops.empty,
          SourceKey.Single, TransSpec1.Id, 2,
          GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
          
        val spec2 = GroupingSource(
          ops.empty,
          SourceKey.Single, TransSpec1.Id, 3,
          GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
          
        val union = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2)

        grouper.findBindingUniverses(union) must haveSize(1)
      }

      "single-source groupings should generate a number of binding universes equal to the number of disjunctive clauses" in {
        val spec = GroupingSource(
          ops.empty,
          SourceKey.Single, SourceValue.Single, 3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))

        grouper.findBindingUniverses(spec) must haveSize(2)
      }
      
      "multiple-source groupings should generate a number of binding universes equal to the product of the number of disjunctive clauses from each source" in {
        val spec1 = GroupingSource(
          ops.empty,
          SourceKey.Single, TransSpec1.Id, 2,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val spec2 = GroupingSource(
          ops.empty,
          SourceKey.Single, TransSpec1.Id, 3,
          GroupKeySpecOr(
            GroupKeySpecSource(JPathField("1"), DerefObjectStatic(Leaf(Source), JPathField("a"))),
            GroupKeySpecSource(JPathField("2"), DerefObjectStatic(Leaf(Source), JPathField("b")))))
          
        val union = GroupingAlignment(
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          DerefObjectStatic(Leaf(Source), JPathField("1")),
          spec1,
          spec2)

        grouper.findBindingUniverses(union) must haveSize(4)
      }
    }

    "derive a correct TransSpec for a conjunctive GroupKeySpec" in {
      val keySpec = GroupKeySpecAnd(
        GroupKeySpecSource(JPathField("1"), DerefObjectStatic(SourceValue.Single, JPathField("a"))),
        GroupKeySpecSource(JPathField("2"), DerefObjectStatic(SourceValue.Single, JPathField("b"))))

      val transspec = grouper.Universe.deriveKeyTransSpec(keySpec)
      val JArray(data) = JsonParser.parse("""[
        {"key": [1], "value": {"a": 12, "b": 7}},
        {"key": [2], "value": {"a": 42}},
        {"key": [1], "value": {"a": 13, "c": true}}
      ]""")

      val JArray(expected) = JsonParser.parse("""[
        [["1", 12], ["2", 7]],
        [["1", 42]],
        [["1", 13]]
      ]""")

      fromJson(data.toStream).transform(transspec).toJson.copoint must_== expected
    }

    "find the maximal spanning forest of a set of merge trees" in {
      import grouper.Universe._

      val abcd = MergeNode(Set("a", "b", "c", "d").map(JPathField(_)))
      val abc = MergeNode(Set("a", "b", "c").map(JPathField(_)))
      val ab = MergeNode(Set("a", "b").map(JPathField(_)))
      val ac = MergeNode(Set("a", "c").map(JPathField(_)))
      val a = MergeNode(Set(JPathField("a")))
      val e = MergeNode(Set(JPathField("e")))

      val connectedNodes = Set(abcd, abc, ab, ac, a)
      val allNodes = connectedNodes + e
      
      val result = findSpanningForest(
        allNodes.map(n => MergeTree(Set(n))),
        (for (n1 <- connectedNodes; n2 <- connectedNodes if n1 != n2) yield MergeEdge(n1, n2, n1.keys & n2.keys)).toList
      )

      result.toList must beLike {
        case MergeTree(n1, e1) :: MergeTree(n2, e2) :: Nil =>
          val (nodes, edges) = if (n1 == Set(e)) (n2, e2) else (n1, e1)

          nodes must haveSize(5)
          edges must haveSize(4) 
          edges.map(_.sharedKey.size) must_== Set(3, 2, 2, 1)
      }
    }

    "binding constraints" >> {
      import grouper.Universe.BindingConstraints._

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
    }

    "graph traversal" >> {
      def norm(s: Set[BindingConstraint]) = s.map(_.ordering.toList)

      val abcd = MergeNode(ticvars("abcd").toSet)
      val abc = MergeNode(ticvars("abc").toSet)
      val ab = MergeNode(ticvars("ab").toSet)
      val ac = MergeNode(ticvars("ac").toSet)
      val a = MergeNode(ticvars("a").toSet)

      "find underconstrained binding constraints" >> {
        "for a graph with singleton sets" in {
          val allNodes = Random.shuffle(Set(abcd, abc, ab, ac, a))

          val spanningForest = findSpanningForest(
            allNodes.map(n => MergeTree(Set(n))),
            (for (n1 <- allNodes; n2 <- allNodes if n1 != n2) yield MergeEdge(n1, n2, n1.keys & n2.keys)).toList
          )

          spanningForest must haveSize(1)

          val underconstrained = spanningForest.head.underconstrained

          norm(underconstrained(a)) must_== norm(Set(constraint("a")))
          norm(underconstrained(ab)) must_== norm(Set(constraint("a,b")))
          norm(underconstrained(ac)) must_== norm(Set(constraint("ac")))
          norm(underconstrained(abc)) must_== norm(Set(constraint("a,b,c"), constraint("ac,b")))
          norm(underconstrained(abcd)) must_== norm(Set(constraint("abcd")))
        }

        "for a graph with no singleton sets" in { 
          val allNodes = Random.shuffle(LinkedHashSet(abcd, abc, ab, ac))

          val spanningForest = findSpanningForest(
            allNodes.map(n => MergeTree(Set(n))).asInstanceOf[Set[MergeTree]],
            (for (n1 <- allNodes; n2 <- allNodes if n1 != n2) yield MergeEdge(n1, n2, n1.keys & n2.keys)).toList
          )

          spanningForest must haveSize(1)
          val tree = spanningForest.head

          val underconstrained = tree.underconstrained

          // These exist for all permutations
          norm(underconstrained(ab)) must_== norm(Set(constraint("ab")))
          norm(underconstrained(ac)) must_== norm(Set(constraint("ac")))
          norm(underconstrained(abcd)) must_== norm(Set(constraint("ab,cd"), constraint("ac,bd")))

          {
            // ab->abcd and ac->abc
            tree.adjacent(ab, abcd) must beTrue
            tree.adjacent(ac, abc) must beTrue
            norm(underconstrained(abc)) must_== norm(Set(constraint("ac,b"))) 
          } or {
            // ac->abcd and ab->abc
            tree.adjacent(ac, abcd) must beTrue
            tree.adjacent(ab, abc) must beTrue
            norm(underconstrained(abc)) must_== norm(Set(constraint("ab,c"))) 
          } or {
            // abcd->abc and (ab, ac)->abcd
            tree.adjacent(abc, abcd) must beTrue
            tree.adjacent(ac, abcd) must beTrue
            tree.adjacent(ab, abcd) must beTrue
            norm(underconstrained(abc)) must_== norm(Set(constraint("abc"))) 
          } or {
            // abc->ab, abc->ac, abcd->abc
            tree.adjacent(abc, abcd) must beTrue
            tree.adjacent(ac, abc) must beTrue
            tree.adjacent(ab, abc) must beTrue
            norm(underconstrained(abc)) must_== norm(Set(constraint("ab,c"), constraint("ac,b"))) 
          }
        }.pendingUntilFixed
      }
    }

    "select constraint matching a set of binding constraints" >> {
      "a preferred match" in {
        val preferred = Set(ticvars("abc"), ticvars("abd"))
        val dispreferred = Set(ticvars("ab"), ticvars("ad"))

        val constraints = Set(
          constraint("ab,c"),
          constraint("a,c,b")
        )

        BindingConstraints.select(preferred, dispreferred, constraints) must_== ticvars("abc")
      }

      "a second-choice match" in {
        val preferred = Set(ticvars("abce"), ticvars("abd"))
        val dispreferred = Set(ticvars("ab"), ticvars("ad"))

        val constraints = Set(
          constraint("ab,c"),
          constraint("a,c,b")
        )

        BindingConstraints.select(preferred, dispreferred, constraints) must_== ticvars("ab")
      }

      "error on no match" in {
        val preferred = Set(ticvars("abce"), ticvars("abd"))
        val dispreferred = Set(ticvars("ad"))

        val constraints = Set(
          constraint("ab,c"),
          constraint("a,c,b")
        )

        BindingConstraints.select(preferred, dispreferred, constraints) must throwA[RuntimeException]
      }
    }

    "generate a trivial merge specification" in {
      // Query:
      // forall 'a 
      //   foo' := foo where foo.a = 'a

      val ticvar = JPathField("a")
      val node = MergeNode(Set(ticvar))
      val tree = MergeTree(Set(node))
      val binding = Binding(ops.empty, SourceKey.Single, TransSpec1.Id, 1, GroupKeySpecSource(ticvar, DerefObjectStatic(SourceValue.Single, ticvar)))

      val result = buildMerges(Map(node -> List(binding)), tree)

      val expected = NodeMergeSpec(
        List(ticvar),
        Set(
          SortMergeSpec(
            SourceMergeSpec(binding),
            WrapArray(
              ArrayConcat(
                WrapArray(ConstLiteral(CString("a"),DerefObjectStatic(SourceValue.Single, ticvar))),
                WrapArray(DerefObjectStatic(SourceValue.Single, ticvar)))))))
      
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
      val tree = MergeTree(Set(foonode, barnode), Set(MergeEdge(foonode, barnode, Set(tica))))
      val foobinding = Binding(ops.empty, SourceKey.Single, TransSpec1.Id, 1, GroupKeySpecSource(tica, DerefObjectStatic(SourceValue.Single, tica)))
      val barbinding = Binding(ops.empty, SourceKey.Single, TransSpec1.Id, 1, 
        GroupKeySpecAnd(
          GroupKeySpecSource(tica, DerefObjectStatic(SourceValue.Single, tica)),
          GroupKeySpecSource(ticb, DerefObjectStatic(SourceValue.Single, ticb))))

      val result = buildMerges(Map(foonode -> List(foobinding), barnode -> List(barbinding)), tree)

      val expected = NodeMergeSpec(
        Vector(tica),
        Set(
          LeftAlignMergeSpec(
            MergeAlignment(
              SortMergeSpec(
                SourceMergeSpec(foobinding),
                WrapArray(
                  ArrayConcat(
                    WrapArray(ConstLiteral(CString("a"),DerefObjectStatic(SourceValue.Single,tica))),
                    WrapArray(DerefObjectStatic(SourceValue.Single,tica))))),
              NodeMergeSpec(
                List(tica, ticb),
                Set(
                  SortMergeSpec(
                    SourceMergeSpec(barbinding),
                    ArrayConcat(
                      WrapArray(ArrayConcat(WrapArray(ConstLiteral(CString("a"),DerefObjectStatic(SourceValue.Single,tica))),WrapArray(DerefObjectStatic(SourceValue.Single,tica)))),
                      WrapArray(ArrayConcat(WrapArray(ConstLiteral(CString("b"),DerefObjectStatic(SourceValue.Single,ticb))),WrapArray(DerefObjectStatic(SourceValue.Single,ticb)))))))),
              Vector(tica)
            ))))

      result must_== expected
    }
  }
}

object ColumnarTableModuleSpec extends ColumnarTableModuleSpec[Free.Trampoline] {
  implicit def M = Trampoline.trampolineMonad
  implicit def coM = Trampoline.trampolineMonad
}


// vim: set ts=4 sw=4 et:
