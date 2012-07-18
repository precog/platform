package com.precog
package daze

import bytecode.RandomLibrary
import common.Path
import yggdrasil._

import blueeyes.json.{ JPath, JPathField, JPathIndex }

import org.specs2.execute.Result
import org.specs2.mutable._

import scalaz._

class TypeInferencerSpec extends Specification
  with TestConfigComponent 
  with ReductionLib 
  with StatsLib
  with MathLib
  with InfixLib
  with MemoryDatasetConsumer {

  import dag._
  import instructions.{
    Line,
    BuiltInFunction2Op,
    Add, Neg,
    DerefArray, DerefObject,
    ArraySwap, WrapObject, JoinObject,
    Map2Cross, Map2CrossLeft, Map2CrossRight, Map2Match,
    PushString, PushNum
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

      case New(loc, parent) => extractLoads(parent)

      case LoadLocal(loc, Root(_, PushString(path)), jtpe) => Map(path -> flattenType(jtpe))

      case Operate(loc, op, parent) => extractLoads(parent)

      case Reduce(loc, red, parent) => extractLoads(parent)

      case Morph1(loc, m, parent) => extractLoads(parent)

      case Morph2(loc, m, left, right) => merge(extractLoads(left), extractLoads(right))

      case Join(loc, instr, left, right) => merge(extractLoads(left), extractLoads(right))

      case Filter(loc, cross, target, boolean) => merge(extractLoads(target), extractLoads(boolean))

      case Sort(parent, indices) => extractLoads(parent)

      case Memoize(parent, priority) => extractLoads(parent)

      case Distinct(loc, parent) => extractLoads(parent)

      case Split(loc, spec, child) => merge(extractSpecLoads(spec), extractLoads(child))
      
      case SplitGroup(_, _, _) | SplitParam(_, _) => Map() 
    }
  }

  "type inference" should {
    "propagate structure/type information through a trivial Join/DerefObject node" in {
      val line = Line(0, "")

      val input =
        Join(line, Map2Cross(DerefObject),
          LoadLocal(line, Root(line, PushString("/file"))),
          Root(line, PushString("column")))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))
      
      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull))
      )

      result must_== expected
    }

    "propagate structure/type information through New nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          New(line,
            Join(line, Map2Cross(DerefObject), 
              LoadLocal(line, Root(line, PushString("/file"))),
              Root(line, PushString("column")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through Operate nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file"))),
            Root(line, PushString("column"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through Reduce nodes" in {
      val line = Line(0, "")

      val input =
        Reduce(line, Mean,
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file"))),
            Root(line, PushString("column"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull))
      )

      result must_== expected
    }

    "propagate structure/type information through Morph1 nodes" in {
      val line = Line(0, "")

      val input =
        Morph1(line, Median,
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file"))),
            Root(line, PushString("column"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through Morph2 nodes" in {
      val line = Line(0, "")

      val input =
        Morph2(line, Covariance,
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file0"))),
            Root(line, PushString("column0"))),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file1"))),
            Root(line, PushString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CLong, CDouble, CDecimalArbitrary)),
        "/file1" -> Map(JPath("column1") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through DerefArray Join nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          New(line,
            Join(line, Map2Cross(DerefArray), 
              LoadLocal(line, Root(line, PushString("/file"))),
              Root(line, PushNum("0")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath(0) -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through ArraySwap Join nodes" in {
      val line = Line(0, "")

      val input =
        Join(line, Map2Cross(ArraySwap),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file0"))),
            Root(line, PushString("column0"))),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file1"))),
            Root(line, PushString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull)),
        "/file1" -> Map(JPath("column1") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through WrapObject Join nodes" in {
      val line = Line(0, "")

      val input =
        Join(line, Map2Cross(WrapObject),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file0"))),
            Root(line, PushString("column0"))),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file1"))),
            Root(line, PushString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull)),
        "/file1" -> Map(JPath("column1") -> Set(CStringArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through Op2 Join nodes" in {
      val line = Line(0, "")

      val input =
        Join(line, Map2Match(BuiltInFunction2Op(min)),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file0"))),
            Root(line, PushString("column0"))),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file0"))),
            Root(line, PushString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(
          JPath("column0") -> Set(CLong, CDouble, CDecimalArbitrary),
          JPath("column1") -> Set(CLong, CDouble, CDecimalArbitrary)
        )
      )

      result must_== expected
    }

    "propagate structure/type information through Filter nodes" in {
      val line = Line(0, "")

      val input =
        Filter(line, None,
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file0"))),
            Root(line, PushString("column0"))),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/file1"))),
            Root(line, PushString("column1"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file0" -> Map(JPath("column0") -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull)),
        "/file1" -> Map(JPath("column1") -> Set(CBoolean))
      )

      result must_== expected
    }

    "propagate structure/type information through Sort nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Sort(
            Join(line, Map2Cross(DerefObject), 
              LoadLocal(line, Root(line, PushString("/file"))),
              Root(line, PushString("column"))),
            Vector()
          )
        )

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through Memoize nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Memoize(
            Join(line, Map2Cross(DerefObject), 
              LoadLocal(line, Root(line, PushString("/file"))),
              Root(line, PushString("column"))),
            23
          )
        )

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }

    "propagate structure/type information through Distinct nodes" in {
      val line = Line(0, "")

      val input =
        Operate(line, Neg,
          Distinct(line,
            Join(line, Map2Cross(DerefObject), 
              LoadLocal(line, Root(line, PushString("/file"))),
              Root(line, PushString("column")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(JPath("column") -> Set(CLong, CDouble, CDecimalArbitrary))
      )

      result must_== expected
    }
    
    "propagate structure/type information through Split nodes" in {
      val line = Line(0, "")

      def clicks = LoadLocal(line, Root(line, PushString("/file")))

      lazy val input: Split =
        Split(line,
          Group(
            1,
            clicks,
            UnfixedSolution(0, 
              Join(line, Map2Cross(DerefObject),
                clicks,
                Root(line, PushString("column0"))))),
          Join(line, Map2Match(Add),
            Join(line, Map2Cross(DerefObject),
              SplitParam(line, 0)(input),
              Root(line, PushString("column1"))),
            Join(line, Map2Cross(DerefObject),
              SplitGroup(line, 1, clicks.provenance)(input),
              Root(line, PushString("column2")))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/file" -> Map(
          JPath.Identity -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull),
          JPath("column0") -> Set(CBoolean, CLong, CDouble, CDecimalArbitrary, CStringArbitrary, CNull)
        )
      )

      result must_== expected
    }
    
    "rewrite loads for a trivial but complete DAG such that they will restrict the columns loaded" in {
      val line = Line(0, "")

      val input =
        Join(line, Map2Match(Add),
          Join(line, Map2Cross(DerefObject), 
            LoadLocal(line, Root(line, PushString("/clicks"))),
            Root(line, PushString("time"))),
          Join(line, Map2Cross(DerefObject),
            LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
            Root(line, PushString("height"))))

      val result = extractLoads(inferTypes(JType.JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(JPath("time") -> Set(CLong, CDouble, CNum)),
        "/hom/heightWeight" -> Map(JPath("height") -> Set(CLong, CDouble, CNum))
      )

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
