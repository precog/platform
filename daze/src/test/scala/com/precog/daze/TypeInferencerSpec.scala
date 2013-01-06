package com.precog
package daze

import bytecode.RandomLibrary
import common.Path
import yggdrasil._

import blueeyes.json.{ JPath, JPathField, JPathIndex }

import org.specs2.execute.Result
import org.specs2.mutable._

import scalaz._

trait TypeInferencerSpec[M[+_]] extends Specification
  with EvaluatorTestSupport[M] 
  with ReductionLib[M] 
  with StatsLib[M]
  with MathLib[M]
  with InfixLib[M]
  with LongIdMemoryDatasetConsumer[M] {

  import dag._
  import instructions.{
    Line,
    BuiltInFunction2Op,
    Add, Neg,
    DerefArray, DerefObject,
    ArraySwap, WrapObject, JoinObject,
    Map2Cross, Map2CrossLeft, Map2CrossRight, Map2Match
  }
  import bytecode._

  def flattenType(jtpe : JType) : Map[JPath, Set[CType]] = {
    def flattenAux(jtpe : JType) : Set[(JPath, Option[CType])] = jtpe match {
      case p : JPrimitiveType => Schema.ctypes(p).map(tpe => (JPath.Identity, Some(tpe)))

      case JArrayFixedT(elems) =>
        for((i, jtpe) <- elems.toSet; (path, ctpes) <- flattenAux(jtpe)) yield (JPathIndex(i) \ path, ctpes)

      case JObjectFixedT(fields) =>
        for((field, jtpe) <- fields.toSet; (path, ctpes) <- flattenAux(jtpe)) yield (JPathField(field) \ path, ctpes)

      case JUnionT(left, right) => flattenAux(left) ++ flattenAux(right)

      case u @ (JArrayUnfixedT | JObjectUnfixedT) => Set((JPath.Identity, None))
    }

    flattenAux(jtpe).groupBy(_._1).mapValues(_.flatMap(_._2))
  }

  def extractLoads(graph : DepGraph): Map[String, Map[JPath, Set[CType]]] = {
    
    def merge(left: Map[String, Map[JPath, Set[CType]]], right: Map[String, Map[JPath, Set[CType]]]): Map[String, Map[JPath, Set[CType]]] = {
      def mergeAux(left: Map[JPath, Set[CType]], right: Map[JPath, Set[CType]]): Map[JPath, Set[CType]] = {
        left ++ right.map { case (path, ctpes) => path -> (ctpes ++ left.getOrElse(path, Set())) }
      }
      left ++ right.map { case (file, jtpes) => file -> mergeAux(jtpes, left.getOrElse(file, Map())) }
    }

    def extractSpecLoads(spec: BucketSpec):  Map[String, Map[JPath, Set[CType]]] = spec match {
      case UnionBucketSpec(left, right) =>
        merge(extractSpecLoads(left), extractSpecLoads(right)) 
      
      case IntersectBucketSpec(left, right) =>
        merge(extractSpecLoads(left), extractSpecLoads(right)) 
      
      case Group(id, target, child) =>
        merge(extractLoads(target), extractSpecLoads(child)) 
      
      case UnfixedSolution(id, target) =>
        extractLoads(target)
      
      case Extra(target) =>
        extractLoads(target)
    }
    
    graph match {
      case _ : Root => Map()

      case New(_, parent) => extractLoads(parent)

      case LoadLocal(_, Const(_, CString(path)), jtpe) => Map(path -> flattenType(jtpe))

      case Operate(_, _, parent) => extractLoads(parent)

      case Reduce(_, _, parent) => extractLoads(parent)

      case Morph1(_, _, parent) => extractLoads(parent)

      case Morph2(_, _, left, right) => merge(extractLoads(left), extractLoads(right))

      case Join(_, _, joinSort, left, right) => merge(extractLoads(left), extractLoads(right))

      case Filter(_, _, target, boolean) => merge(extractLoads(target), extractLoads(boolean))

      case Sort(parent, _) => extractLoads(parent)

      case SortBy(parent, _, _, _) => extractLoads(parent)

      case Memoize(parent, _) => extractLoads(parent)

      case Distinct(_, parent) => extractLoads(parent)

      case Split(_, spec, child) => merge(extractSpecLoads(spec), extractLoads(child))
      
      case SplitGroup(_, _, _) | SplitParam(_, _) => Map() 
    }
  }

  "type inference" should {
    "propagate structure/type information through a trivial Join/DerefObject node" in {
      val line = Line(0, "")

      val input =
        Join(line, DerefObject, CrossLeftSort,
          LoadLocal(line, Const(line, CString("/file"))),
          Const(line, CString("column")))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))
      
      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull))
      )

      result must_== expected
    }

    "propagate structure/type information through New nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          New(line,
            Join(line, DerefObject, CrossLeftSort, 
              LoadLocal(line, Const(line, CString("/file"))),
              Const(line, CString("column")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Operate nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file"))),
            Const(line, CString("column"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Reduce nodes" in {
      val line = Line(0, "")

      val input =
        Reduce(line, Mean,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file"))),
            Const(line, CString("column"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Morph1 nodes" in {
      val line = Line(0, "")

      val input =
        Morph1(line, Median,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file"))),
            Const(line, CString("column"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Morph2 nodes" in {
      val line = Line(0, "")

      val input =
        Morph2(line, Covariance,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file0"))),
            Const(line, CString("column0"))),
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file1"))),
            Const(line, CString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CLong, CDouble, CNum)),
        "/file1" -> Map(JPath("column1") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through DerefArray Join nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          New(line,
            Join(line, DerefArray, CrossLeftSort, 
              LoadLocal(line, Const(line, CString("/file"))),
              Const(line, CLong(0)))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath(0) -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through ArraySwap Join nodes" in {
      val line = Line(0, "")

      val input =
        Join(line, ArraySwap, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file0"))),
            Const(line, CString("column0"))),
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file1"))),
            Const(line, CString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set[CType]()),
        "/file1" -> Map(JPath("column1") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through WrapObject Join nodes" in {
      val line = Line(0, "")

      val input =
        Join(line, WrapObject, CrossLeftSort,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file0"))),
            Const(line, CString("column0"))),
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file1"))),
            Const(line, CString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CString)),
        "/file1" -> Map(JPath("column1") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull))
      )

      result must_== expected
    }

    "propagate structure/type information through Op2 Join nodes" in {
      val line = Line(0, "")

      val input =
        Join(line, BuiltInFunction2Op(min), IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file0"))),
            Const(line, CString("column0"))),
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file0"))),
            Const(line, CString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(
          JPath("column0") -> Set(CLong, CDouble, CNum),
          JPath("column1") -> Set(CLong, CDouble, CNum)
        )
      )

      result must_== expected
    }

    "propagate structure/type information through Filter nodes" in {
      val line = Line(0, "")

      val input =
        Filter(line, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file0"))),
            Const(line, CString("column0"))),
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/file1"))),
            Const(line, CString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull)),
        "/file1" -> Map(JPath("column1") -> Set(CBoolean))
      )

      result must_== expected
    }

    "propagate structure/type information through Sort nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Sort(
            Join(line, DerefObject, CrossLeftSort, 
              LoadLocal(line, Const(line, CString("/file"))),
              Const(line, CString("column"))),
            Vector()
          )
        )

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through SortBy nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          SortBy(
            Join(line, DerefObject, CrossLeftSort, 
              LoadLocal(line, Const(line, CString("/file"))),
              Const(line, CString("column"))),
            "foo", "bar", 23
          )
        )

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Memoize nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Memoize(
            Join(line, DerefObject, CrossLeftSort, 
              LoadLocal(line, Const(line, CString("/file"))),
              Const(line, CString("column"))),
            23
          )
        )

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }

    "propagate structure/type information through Distinct nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Distinct(line,
            Join(line, DerefObject, CrossLeftSort, 
              LoadLocal(line, Const(line, CString("/file"))),
              Const(line, CString("column")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }
    
    "propagate structure/type information through Split nodes (1)" in {
      val line = Line(0, "")

      def clicks = LoadLocal(line, Const(line, CString("/file")))

      lazy val input: Split =
        Split(line,
          Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Const(line, CString("column0"))))),
          Join(line, Add, CrossLeftSort,
            Join(line, DerefObject, CrossLeftSort,
              SplitParam(line, 0)(input),
              Const(line, CString("column1"))),
            Join(line, DerefObject, CrossLeftSort,
              SplitGroup(line, 1, clicks.identities)(input),
              Const(line, CString("column2")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(
          JPath.Identity -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull),
          JPath("column0") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull),
          JPath("column0.column1") -> Set(CLong, CDouble, CNum),
          JPath("column2") -> Set(CLong, CDouble, CNum)
        )
      )

      result mustEqual expected
    }
    
    "propagate structure/type information through Split nodes (2)" in {
      val line = Line(0, "")
      def clicks = LoadLocal(line, Const(line, CString("/clicks")))
      
      // clicks := //clicks forall 'user { user: 'user, num: count(clicks.user where clicks.user = 'user) }
      lazy val input: Split =
        Split(line,
          Group(0,
            Join(line, DerefObject, CrossLeftSort, clicks, Const(line, CString("user"))),
            UnfixedSolution(1,
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Const(line, CString("user"))))),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, WrapObject, CrossLeftSort,
              Const(line, CString("user")),
              SplitParam(line, 1)(input)),
            Join(line, WrapObject, CrossLeftSort,
              Const(line, CString("num")),
              Reduce(line, Count,
                SplitGroup(line, 0, clicks.identities)(input)))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(
          JPath("user") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull)
        )
      )

      result must_== expected
    }
    
    "propagate structure/type information through Split nodes (3)" in {
      val line = Line(0, "")
      def clicks = LoadLocal(line, Const(line, CString("/clicks")))
      
      // clicks := //clicks forall 'user { user: 'user, age: clicks.age, num: count(clicks.user where clicks.user = 'user) }
      lazy val input: Split =
        Split(line,
          Group(0,
            Join(line, DerefObject, CrossLeftSort, clicks, Const(line, CString("user"))),
            UnfixedSolution(1,
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Const(line, CString("user"))))),
          Join(line, JoinObject, CrossLeftSort,
            Join(line, JoinObject, CrossLeftSort,
              Join(line, WrapObject, CrossLeftSort,
                Const(line, CString("user")),
                SplitParam(line, 1)(input)),
              Join(line, WrapObject, CrossLeftSort,
                Const(line, CString("num")),
                Reduce(line, Count,
                  SplitGroup(line, 0, clicks.identities)(input)))),
            Join(line, WrapObject, CrossLeftSort,
              Const(line, CString("age")),
              Join(line, DerefObject, CrossLeftSort,
                clicks,
                Const(line, CString("age"))))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(
          JPath("user") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull),
          JPath("age") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull)
        )
      )

      result must_== expected
    }

    "rewrite loads for a trivial but complete DAG such that they will restrict the columns loaded" in {
      val line = Line(0, "")

      val input =
        Join(line, Add, IdentitySort,
          Join(line, DerefObject, CrossLeftSort, 
            LoadLocal(line, Const(line, CString("/clicks"))),
            Const(line, CString("time"))),
          Join(line, DerefObject, CrossLeftSort,
            LoadLocal(line, Const(line, CString("/hom/heightWeight"))),
            Const(line, CString("height"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(JPath("time") -> Set(CLong, CDouble, CNum)),
        "/hom/heightWeight" -> Map(JPath("height") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }
    
    "negate type inference from deref by wrap" in {
      val line = Line(0, "")
      
      val clicks = LoadLocal(line, Const(line, CString("/clicks")))
      
      val input =
        Join(line, DerefObject, CrossLeftSort,
          Join(line, WrapObject, CrossLeftSort,
            Const(line, CString("foo")),
            clicks),
          Const(line, CString("foo")))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(JPath.Identity -> Set(CBoolean, CLong, CDouble, CString, CNum, CNull)))

      result mustEqual expected
    }
    
    "propagate type information through split->wrap->deref" in {
      val line = Line(0, "")
      
      val clicks = LoadLocal(line, Const(line, CString("/clicks")))
      
      val clicksTime =
        Join(line, DerefObject, CrossLeftSort,
          clicks,
          Const(line, CString("time")))
      
      lazy val split: dag.Split =
        Split(line,
          Group(0, clicks, UnfixedSolution(1, clicksTime)),
          Join(line, WrapObject, CrossLeftSort,
            Const(line, CString("foo")),
            SplitGroup(line, 0, Identities.Specs(Vector(LoadIds("/clicks"))))(split)))
            
      val input =
        Join(line, DerefObject, CrossLeftSort,
          split,
          Const(line, CString("foo")))
      
      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))
      
      val expected = Map(
        "/clicks" -> Map(
          JPath.Identity -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull),
          JPath("time") -> Set(CBoolean, CLong, CDouble, CNum, CString, CNull)))
        
      result mustEqual expected
    }
  }
}

object TypeInferencerSpec extends TypeInferencerSpec[test.YId] with test.YIdInstances

// vim: set ts=4 sw=4 et:

