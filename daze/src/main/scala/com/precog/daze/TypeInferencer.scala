package com.precog
package daze

import bytecode._
import yggdrasil._

trait TypeInferencer extends DAG {
  import instructions.{
    BinaryOperation, ArraySwap, WrapArray, WrapObject, DerefArray, DerefObject,
    JoinInstr, Map2, Map2Cross, Map2CrossLeft, Map2CrossRight
  }
  import dag._

  def inferTypes(jtpe: JType)(graph: DepGraph) : DepGraph = {

    def inferSpecTypes(jtpe: JType, splits: => Map[Split, Split])(spec: BucketSpec): BucketSpec = spec match {
      case UnionBucketSpec(left, right) =>
        UnionBucketSpec(inferSpecTypes(jtpe, splits)(left), inferSpecTypes(jtpe, splits)(right))
      
      case IntersectBucketSpec(left, right) =>
        IntersectBucketSpec(inferSpecTypes(jtpe, splits)(left), inferSpecTypes(jtpe, splits)(right))
      
      case Group(id, target, child) =>
        Group(id, inferTypesAux(jtpe, splits)(target), inferSpecTypes(jtpe, splits)(child))
      
      case UnfixedSolution(id, target) =>
        UnfixedSolution(id, inferTypesAux(jtpe, splits)(target))
      
      case Extra(target) =>
        Extra(inferTypesAux(jtpe, splits)(target))
    }

    def inferTypesAux(jtpe: JType, splits0: => Map[Split, Split])(graph: DepGraph) : DepGraph = {
      lazy val splits = splits0

      graph match {
        case r : Root => r
  
        case New(loc, parent) => New(loc, inferTypesAux(jtpe, splits)(parent))
  
        case LoadLocal(loc, parent, _) => LoadLocal(loc, parent, jtpe)
  
        case Operate(loc, op, parent) => Operate(loc, op, inferTypesAux(op.tpe.arg, splits)(parent))
  
        case Reduce(loc, red, parent) => Reduce(loc, red, inferTypesAux(red.tpe.arg, splits)(parent))
  
        case Morph1(loc, m, parent) => Morph1(loc, m, inferTypesAux(m.tpe.arg, splits)(parent))
  
        case Morph2(loc, m, left, right) => Morph2(loc, m, inferTypesAux(m.tpe.arg0, splits)(left), inferTypesAux(m.tpe.arg1, splits)(right))
  
        case Join(loc, instr @ (Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject)), left, right @ ConstString(str)) =>
          Join(loc, instr, inferTypesAux(JObjectFixedT(Map(str -> jtpe)), splits)(left), right)
  
        case Join(loc, instr @ (Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray)), left, right @ ConstDecimal(d)) =>
          Join(loc, instr, inferTypesAux(JArrayFixedT(Map(d.toInt -> jtpe)), splits)(left), right)
  
        case Join(loc, instr @ (Map2Cross(WrapObject) | Map2CrossLeft(WrapObject) | Map2CrossRight(WrapObject)), left, right) =>
          Join(loc, instr, inferTypesAux(JTextT, splits)(left), inferTypesAux(jtpe, splits)(right))
  
        case Join(loc, instr @ (Map2Cross(ArraySwap) | Map2CrossLeft(ArraySwap) | Map2CrossRight(ArraySwap)), left, right) =>
          Join(loc, instr, inferTypesAux(jtpe, splits)(left), inferTypesAux(JNumberT, splits)(right))
  
        case Join(loc, instr @ Map2(BinaryOperationType(lhs, rhs, res)), left, right) =>
          Join(loc, instr, inferTypesAux(lhs, splits)(left), inferTypesAux(rhs, splits)(right))
  
        case Join(loc, instr, left, right) => Join(loc, instr, inferTypesAux(jtpe, splits)(left), inferTypesAux(jtpe, splits)(right))
  
        case Filter(loc, cross, target, boolean) =>
          Filter(loc, cross, inferTypesAux(jtpe, splits)(target), inferTypesAux(JBooleanT, splits)(boolean))
  
        case Sort(parent, indices) => Sort(inferTypesAux(jtpe, splits)(parent), indices)
  
        case Memoize(parent, priority) => Memoize(inferTypesAux(jtpe, splits)(parent), priority)
  
        case Distinct(loc, parent) => Distinct(loc, inferTypesAux(jtpe, splits)(parent))
  
        case s @ Split(loc, spec, child) => {
          lazy val splits2 = splits + (s -> s2)
          lazy val spec2 = inferSpecTypes(jtpe, splits2)(spec)
          lazy val child2 = inferTypesAux(jtpe, splits2)(child)
          lazy val s2: Split = Split(loc, spec2, child2)
          s2
        }
  
        case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(splits(s.parent))
  
        case s @ SplitParam(loc, id) => SplitParam(loc, id)(splits(s.parent))
      }
    }
    
    inferTypesAux(jtpe, Map())(graph)
  }
}
